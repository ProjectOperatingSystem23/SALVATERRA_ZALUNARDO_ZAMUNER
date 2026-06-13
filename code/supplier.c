/* ============================================================================
 * supplier.c  --  Processo Supplier del Fulfillment Center (Project 2026-3)
 *
 * Uso:
 *   ./supplier <supplier_id> <config_file>
 *
 * COSA FA (spec 2.2.6):
 *   Legge dal proprio file di configurazione (CSV generato da bootstrap.sh)
 *   la lista degli item che rifornisce:
 *       item_id,quantity_per_shipment,interval_seconds
 *   e poi, periodicamente, invia un RestockMsg per ogni item sulla
 *   RESTOCK_FIFO del warehouse. Ogni item ha il SUO intervallo.
 *
 * SCELTE DI PROGETTO:
 *   - Scheduling con COUNTDOWN per item: ad ogni giro il processo dorme per
 *     il minimo countdown residuo (una sola sleep, niente polling), poi
 *     sottrae il tempo dormito a tutti i countdown e spedisce gli item
 *     arrivati a zero. sleep() interrotta da un segnale ritorna i secondi
 *     residui (Lab03): il tempo realmente trascorso e' min_cd - left, e la
 *     terminazione via SIGTERM e' reattiva. Un countdown sceso a 0 o sotto
 *     fa scattare subito la consegna e viene riarmato a interval: non puo'
 *     accumulare ritardo.
 *   - Parsing .conf con sscanf: compatto, verifica i 3 campi in una chiamata.
 *     Strict: una riga malformata fa fallire l'avvio (il .conf e' generato
 *     da bootstrap.sh, se e' rotto c'e' un bug a monte).
 *   - Validazione header obbligatoria con strncmp (Lab06).
 *   - Apertura FIFO con retry su EINTR.
 *   - SIGPIPE gestito con un HANDLER (sigaction, Lab03), non ignorato: il
 *     default di SIGPIPE terminerebbe il processo; con l'handler installato
 *     il processo sopravvive, l'handler alza g_stop e la write che ha
 *     generato il segnale fallisce con EPIPE, gestito al punto di chiamata.
 *   - I segnali NON sono bloccati (processo single-thread): gli handler
 *     settano solo flag volatile sig_atomic_t (Lab03).
 *   - La write su FIFO e' atomica (sizeof(RestockMsg)=12 < PIPE_BUF): piu'
 *     supplier concorrenti non mischiano i messaggi (man 7 pipe).
 *
 * Riferimenti: Lab03 (segnali), Lab05 (fd/IO), Lab06 (FIFO, strncmp).
 *              Parsing .conf con sscanf (C standard, non nei lab: scelta
 *              dichiarata, vedi commento in load_config).
 * ============================================================================ */

#define _POSIX_C_SOURCE 200809L

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <errno.h>
#include "common.h"

#define MAX_CONF_ITEMS 1024
#define LINE_BUF        256
#define CONF_HEADER     "item_id,quantity_per_shipment,interval_seconds"

/* Una riga del file di configurazione + countdown alla prossima consegna. */
typedef struct {
    int item_id;
    int quantity;
    int interval;     /* secondi tra una consegna e la successiva */
    int countdown;    /* secondi mancanti alla prossima consegna  */
} SupplyPlan;

/* Flag di terminazione: settato dagli handler (Lab03). */
static volatile sig_atomic_t g_stop = 0;

/* SIGTERM/SIGINT/SIGPIPE: richiesta di terminazione pulita. */
static void handle_stop(int sig) { (void)sig; g_stop = 1; }

/* SIGPIPE: il warehouse ha chiuso il lato lettura della FIFO (e' morto).
 * L'handler DEVE esistere: il comportamento di default di SIGPIPE e'
 * terminare il processo. Con l'handler installato la write che ha generato
 * il segnale ritorna -1 con errno=EPIPE (gestito al punto di chiamata);
 * qui in piu' alziamo g_stop, perche' senza warehouse non c'e' altro da fare. */

static void setup_handler(int sig, void (*fn)(int))
{
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = fn;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;              /* niente SA_RESTART: la sleep va interrotta */
    sigaction(sig, &sa, NULL);
}

/* ====== I/O di basso livello (Lab05) ======================================= */

/* write "completa" con gestione EINTR (Lab05). */
static ssize_t write_all(int fd, const void *buf, size_t len)
{
    size_t done = 0;
    while (done < len) {
        ssize_t n = write(fd, (const char *)buf + done, len - done);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        done += (size_t)n;
    }
    return (ssize_t)done;
}

/* Legge una riga da fd byte per byte. Ritorna bytes letti, 0=EOF, -1=errore. */
static ssize_t fd_read_line(int fd, char *buf, size_t size)
{
    size_t i = 0;
    while (i < size - 1) {
        char c;
        ssize_t n = read(fd, &c, 1);
        if (n < 0) { if (errno == EINTR) continue; return -1; }
        if (n == 0) break;
        buf[i++] = c;
        if (c == '\n') break;
    }
    buf[i] = '\0';
    return (ssize_t)i;
}

/* ====== Parsing del .conf =================================================== */
static int load_config(const char *path, SupplyPlan *plan, int max_items)
{
    int fd = open(path, O_RDONLY);
    if (fd < 0) {
        fprintf(stderr, "[SUPPLIER] open config '%s': %s\n",
                path, strerror(errno));
        return -1;
    }

    char line[LINE_BUF];

    /* Header obbligatorio (strncmp, Lab06): tollera \r\n finale.
     * Il .conf e' generato da bootstrap.sh: header diverso = file rotto. */
    if (fd_read_line(fd, line, sizeof(line)) <= 0 ||
        strncmp(line, CONF_HEADER, strlen(CONF_HEADER)) != 0) {
        fprintf(stderr, "[SUPPLIER] '%s': header mancante o non valido "
                        "(atteso '%s')\n", path, CONF_HEADER);
        close(fd);
        return -1;
    }

    int count = 0;
    ssize_t n;
    while (count < max_items && (n = fd_read_line(fd, line, sizeof(line))) > 0) {
        if (line[0] == '\r' || line[0] == '\n' || line[0] == '\0') continue;

        /* sscanf: legge i 3 campi interi e verifica che siano tutti presenti
         * (ritorna il numero di conversioni riuscite). Nota per il report:
         * sscanf non compare nei lab ma e' C standard; l'alternativa
         * lab-only sarebbe strtok+atoi (Lab02). */
        SupplyPlan *it = &plan[count];
        if (sscanf(line, "%d,%d,%d",
                   &it->item_id, &it->quantity, &it->interval) != 3) {
            fprintf(stderr, "[SUPPLIER] '%s': riga malformata: %s",
                    path, line);   /* line contiene gia' il suo \n */
            close(fd);
            return -1;
        }
        if (it->item_id < 1 || it->quantity < 1 || it->interval < 1) {
            fprintf(stderr, "[SUPPLIER] '%s': valori non positivi: %s",
                    path, line);
            close(fd);
            return -1;
        }
        it->countdown = it->interval;   /* prima consegna dopo 1 intervallo */
        count++;
    }
    if (n < 0) {
        fprintf(stderr, "[SUPPLIER] read '%s': %s\n", path, strerror(errno));
        close(fd);
        return -1;
    }
    close(fd);
    if (count == 0) {
        fprintf(stderr, "[SUPPLIER] '%s': nessun item valido\n", path);
        return -1;
    }
    return count;
}

/* ====== MAIN ================================================================ */
int main(int argc, char *argv[])
{
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <supplier_id> <config_file>\n", argv[0]);
        return ERR_USAGE;
    }
    int supplier_id = atoi(argv[1]);
    if (supplier_id <= 0) {
        fprintf(stderr, "[SUPPLIER] supplier_id deve essere >= 1\n");
        return ERR_USAGE;
    }

    /* Segnali (Lab03): tutti via sigaction, tutti con handler che settano
     * solo flag. SIGTERM/SIGINT = terminazione richiesta; SIGPIPE =
     * warehouse morto (la write fallira' con EPIPE). */
    setup_handler(SIGTERM, handle_stop);
    setup_handler(SIGINT,  handle_stop);
    setup_handler(SIGPIPE, handle_stop);

    SupplyPlan plan[MAX_CONF_ITEMS];
    int n_items = load_config(argv[2], plan, MAX_CONF_ITEMS);
    if (n_items < 0) return ERR_IO;

    /* Apertura BLOCCANTE della RESTOCK_FIFO in scrittura: aspetta che il
     * warehouse abbia aperto il lato lettura (Lab06). bootstrap.sh avvia
     * il warehouse PRIMA dei supplier, quindi l'attesa e' breve.
     * Essendo bloccante, la open puo' essere interrotta da un segnale
     * (EINTR): in quel caso riproviamo, a meno che non sia un segnale di
     * terminazione (g_stop). Stesso trattamento di read/write_all (Lab05). */
    int fifo_fd = -1;
    while (fifo_fd < 0 && !g_stop) {
        fifo_fd = open(RESTOCK_FIFO, O_WRONLY);
        if (fifo_fd < 0) {
            if (errno == EINTR) continue;
            fprintf(stderr, "[SUPPLIER %d] open '%s': %s\n",
                    supplier_id, RESTOCK_FIFO, strerror(errno));
            return ERR_IO;
        }
    }
    if (fifo_fd < 0) return ERR_OK;   /* SIGTERM arrivato durante l'open */



    while (!g_stop) {
        /* 1. trova il minimo countdown: e' quanto possiamo dormire */
        int min_cd = plan[0].countdown;
        for (int i = 1; i < n_items; i++)
            if (plan[i].countdown < min_cd) min_cd = plan[i].countdown;

        /* 2. dormi: sleep ritorna i secondi NON dormiti se interrotta da un
         *    segnale (es. SIGTERM) -> slept e' il tempo realmente trascorso */
        unsigned int left  = sleep((unsigned int)min_cd);
        int          slept = min_cd - (int)left;
        if (g_stop) break;

        /* 3. aggiorna i countdown e spedisci gli item arrivati a zero */
        for (int i = 0; i < n_items; i++) {
            plan[i].countdown -= slept;
            /* countdown >= 0 per costruzione (slept <= min_cd <= countdown), non serve fare clipping a 0*/
            if (plan[i].countdown > 0) continue;

            RestockMsg msg = { supplier_id, plan[i].item_id, plan[i].quantity };
            if (write_all(fifo_fd, &msg, sizeof(msg)) < 0) {
                /* EPIPE: warehouse morto (l'handler di SIGPIPE ha gia'
                 * alzato g_stop) -> inutile continuare (spec 2.2.10) */
                if (errno == EPIPE)
                    fprintf(stderr, "[SUPPLIER %d] warehouse terminato, esco\n",
                            supplier_id);
                else
                    fprintf(stderr, "[SUPPLIER %d] write su FIFO: %s\n",
                            supplier_id, strerror(errno));
                close(fifo_fd);
                return ERR_WAREHOUSE_DOWN;
            }

           plan[i].countdown = plan[i].interval;   /* riarma il timer */
        }
    }

    close(fifo_fd);

    return ERR_OK;
}