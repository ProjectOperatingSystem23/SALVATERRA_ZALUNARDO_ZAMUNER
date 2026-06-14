/* ============================================================================
 * order_client.c  --  Helper C dello script order.sh (Project 2026-3)
 *
 * Uso (invocato da order.sh, NON direttamente dall'utente finale):
 *   ./order_client <client_id> <item_id> <quantity>
 *
 * RUOLO (spec 2.2.2 / 2.3 "Order script"):
 *   E' il lato CLIENT del protocollo richiesta/risposta sulle FIFO. Bash non sa
 *   scrivere/leggere struct binarie in modo affidabile, quindi l'IPC vero e'
 *   fatto qui in C, riusando ESATTAMENTE le struct di common.h (stessa
 *   interfaccia binaria del warehouse). Flusso:
 *       1. crea una FIFO di risposta PRIVATA  /tmp/order_resp_<PID>
 *       2. APRE la propria FIFO in lettura (prima di inviare: vedi protocollo)
 *       3. invia un OrderRequest sulla ORDERS_FIFO del warehouse
 *       4. attende l'OrderResponse sulla propria FIFO privata
 *       5. traduce la risposta in testo e ritorna lo status come EXIT CODE
 *          (gli stessi ERR_* di common.h, leggibili da $? in Bash: spec 2.2.9)
 *
 * GERARCHIA DI VALIDAZIONE (chi controlla cosa, per non duplicare la logica):
 *   - order.sh    : input lato utente (charset client_id, item_id>=1, qty>=1).
 *   - order_client: SOLO la sicurezza del wire-format (client_id non vuoto e che
 *                   entri nel campo della struct). Per il resto e' un trasporto
 *                   "prudente ma fidato": item_id/quantity passano cosi' come
 *                   sono al warehouse, che e' l'autorita' semantica.
 *   - warehouse   : verita' su quantita' (<=0 -> ERR_INVALID_QTY), esistenza
 *                   item (-> ERR_ITEM_NOT_FOUND), stock (-> ERR_OUT_OF_STOCK /
 *                   ERR_PARTIAL). Tenere qui questi controlli e' DIFESA IN
 *                   PROFONDITA' per chi chiamasse order_client direttamente.
 *
 * PROTOCOLLO FIFO PRIVATA -- il punto delicato (DEVE combaciare con warehouse.c):
 *   Il warehouse, in send_response(), apre la resp_fifo in O_WRONLY|O_NONBLOCK:
 *   se in quel momento NON c'e' gia' un lettore, la open fallisce (ENXIO) e la
 *   risposta va persa. Percio' order_client DEVE avere il lato lettura della
 *   resp_fifo gia' aperto PRIMA di inviare l'OrderRequest, e tenerlo aperto fino
 *   alla lettura della risposta. Usiamo lo STESSO schema di open_fifo_rw del
 *   warehouse:
 *       read-end O_NONBLOCK  -> la open non blocca anche senza writer
 *       write-end "dummy"    -> c'e' SEMPRE >=1 writer: niente EOF spurio nella
 *                               finestra (picker/packer dormono 1-3s, spec 2.2.4)
 *                               prima che il warehouse risponda
 *       fcntl(F_SETFL)       -> toglie O_NONBLOCK: la read finale e' bloccante
 *   NB: fcntl(F_GETFL/F_SETFL) NON compare nei lab (li' c'e' solo
 *   #include <fcntl.h> per i flag di open): e' una deviazione DICHIARATA, identica
 *   a quella che il warehouse usa in open_fifo_rw. La teniamo per coerenza interna
 *   col progetto (da segnalare nel report).
 *
 * SEGNALI (Lab03, tutti via sigaction in setup_handler, come supplier.c):
 *   - SIGALRM: timeout di sicurezza. Se il warehouse muore DOPO aver accettato la
 *     richiesta, la read sulla resp_fifo non vedrebbe mai EOF (per via della dummy
 *     write-end): l'alarm interrompe la syscall (EINTR) e usciamo con ERR_TIMEOUT.
 *     Handler SENZA SA_RESTART, altrimenti la read verrebbe riavviata e il timeout
 *     sarebbe inefficace.
 *   - SIGPIPE: gestito con un HANDLER (come supplier.c, non SIG_IGN): il default
 *     ucciderebbe il processo. Se il warehouse muore mentre scriviamo la richiesta,
 *     la write ritorna -1/EPIPE (gestito -> ERR_WAREHOUSE_DOWN) invece di terminarci.
 *     Per order_client il comportamento osservabile e' identico a SIG_IGN (qui non
 *     c'e' un loop da interrompere): la scelta dell'handler e' per COERENZA con
 *     supplier.c. Nel warehouse invece SIGPIPE e' IGNORATO, perche' li' un client
 *     morto deve far fallire solo quella write, non fermare il server: stessa
 *     primitiva, scelta opposta motivata dal contesto.
 *
 * I/O di basso livello (open/read/write/close), niente <stdio.h> per l'IPC:
 *   stesso livello di warehouse/supplier (Lab05). printf/fprintf solo per
 *   l'output a video. La write della richiesta e' ATOMICA: sizeof(OrderRequest)
 *   = 328 byte < PIPE_BUF (4096), quindi piu' order.sh concorrenti non mischiano
 *   i messaggi sulla ORDERS_FIFO (man 7 pipe); inoltre 328 << 64KB (capacita' del
 *   buffer della pipe) quindi la write non blocca mai.
 *
 *    L'alarm deve coprire tutta la catena — se lo disarmassimo dopo la write, il
 *    caso "warehouse muore dopo aver ricevuto l'ordine ma prima di rispondere" non
 *    sarebbe coperto e la read_response rimarrebbe bloccata per sempre
 *    (per via della dummy write-end che tiene la FIFO aperta)
 *
 * Riferimenti: Lab03 (segnali, alarm, sigaction), Lab05 (fd/IO, EINTR),
 *              Lab06 (FIFO: mkfifo, semantica di blocco e O_NONBLOCK).
 * ============================================================================ */

#define _POSIX_C_SOURCE 200809L

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <errno.h>
#include <sys/stat.h>     /* mkfifo */
#include "common.h"

/* Attesa massima (secondi) per la risposta del warehouse: rete di sicurezza per
 * un warehouse morto a meta'. Generosa perche' sotto carico l'ordine puo' restare
 * in coda (pending -> picker 1-3s -> packaging -> packer 1-3s, spec 2.2.4/2.2.7). */
#define RESP_TIMEOUT_SECONDS  30

/* ====== Flag settati dagli handler (Lab03): solo sig_atomic_t volatile ====== */
static volatile sig_atomic_t g_timed_out = 0;   /* alzato da SIGALRM            */
static volatile sig_atomic_t g_pipe      = 0;   /* alzato da SIGPIPE (wh morto) */

static void on_alarm(int sig) { (void)sig; g_timed_out = 1; }
static void on_pipe (int sig) { (void)sig; g_pipe      = 1; }

/* ====== I/O di basso livello (Lab05) ======================================= */


/* read "completa" della risposta, SENSIBILE AL TIMEOUT. A differenza della
 * read_all del warehouse, su EINTR NON riprendiamo ciecamente: se l'interruzione
 * e' dovuta a SIGALRM (g_timed_out) abbandoniamo, cosi' il timeout e' effettivo.
 * Ritorna i byte letti, 0 = EOF (non atteso: teniamo la dummy write), -1 = errore. */
static ssize_t read_response(int fd, void *buf, size_t len)
{
    size_t done = 0;
    while (done < len) {
        ssize_t n = read(fd, (char *)buf + done, len - done);
        if (n < 0) {
            if (errno == EINTR) {
                if (g_timed_out) return -1;   /* timeout scaduto: molla        */
                continue;                     /* altro segnale: riprova        */
            }
            return -1;
        }
        if (n == 0) break;                    /* EOF (non dovrebbe capitare)    */
        done += (size_t)n;
    }
    return (ssize_t)done;
}

/* ====== Traduzione status -> testo leggibile (output per l'utente) ========= */
/* Il warehouse risponde in BINARIO (OrderResponse); qui lo rendiamo leggibile.
 * I case coprono tutti gli status che il warehouse puo' mettere in resp.status
 * (vedi receiver_thread / packer in warehouse.c). */
static void print_outcome(const char *client_id, const OrderResponse *r)
{
    switch (r->status) {
    case ERR_OK:
        printf("[OK] %s: ordine COMPLETO  item=%d  spedite=%d/%d\n",
               client_id, r->item_id, r->qty_shipped, r->qty_requested);
        break;
    case ERR_PARTIAL:
        printf("[PARZIALE] %s: item=%d  spedite=%d/%d  (rifiutate=%d, stock insufficiente)\n",
               client_id, r->item_id, r->qty_shipped, r->qty_requested, r->qty_rejected);
        break;
    case ERR_ITEM_NOT_FOUND:
        printf("[RIFIUTATO] %s: item=%d inesistente in inventario\n",
               client_id, r->item_id);
        break;
    case ERR_OUT_OF_STOCK:
        printf("[RIFIUTATO] %s: item=%d esaurito (out of stock)\n",
               client_id, r->item_id);
        break;
    case ERR_INVALID_QTY:
        printf("[RIFIUTATO] %s: quantita' non valida (%d)\n",
               client_id, r->qty_requested);
        break;
    default:
        printf("[RIFIUTATO] %s: item=%d  status=%d\n",
               client_id, r->item_id, r->status);
        break;
    }
}

/* ====== MAIN ================================================================ */
int main(int argc, char *argv[])
{
    if (argc != 4) {
        fprintf(stderr, "Usage: %s <client_id> <item_id> <quantity>\n", argv[0]);
        return ERR_USAGE;
    }
    const char *client_id = argv[1];
    int item_id  = atoi(argv[2]);   /* order.sh ha gia' validato; atoi e' Lab02   */
    int quantity = atoi(argv[3]);   /* eventuali valori <=0 li valida il warehouse */

    /* Sicurezza del wire-format: client_id deve entrare nel campo della struct
     * (MAX_CLIENT_ID, NUL incluso). L'helper non si fida del chiamante. */
    if (client_id[0] == '\0' || strlen(client_id) >= MAX_CLIENT_ID) {
        fprintf(stderr, "[ORDER] client_id mancante o troppo lungo (max %d)\n",
                MAX_CLIENT_ID - 1);
        return ERR_USAGE;
    }

    /* Segnali (Lab03): timeout + sopravvivenza a un warehouse che muore. */
    setup_handler(SIGALRM, on_alarm);
    setup_handler(SIGPIPE, on_pipe);

    /* ---- 1. FIFO di risposta privata: /tmp/order_resp_<PID> ---- */
    char resp_path[MAX_RESP_FIFO];
    snprintf(resp_path, sizeof(resp_path), RESP_FIFO_TEMPLATE, (int)getpid());

    int rfd, wdummy;
    if (open_fifo_rw(resp_path, 0600, &rfd, &wdummy) != 0) {
        fprintf(stderr, "[ORDER] init resp_fifo '%s': %s\n", resp_path, strerror(errno));
        unlink(resp_path);              /* se mkfifo l'aveva creata, la rimuoviamo */
        return ERR_IO;
    }

    /* Da qui armiamo il timeout GLOBALE: copre open(ORDERS_FIFO), write e read. */
    alarm(RESP_TIMEOUT_SECONDS);

    /* ---- 3. apri la ORDERS_FIFO in scrittura (bloccante) ----
     * Aspetta che il warehouse abbia il lato lettura aperto (lo tiene sempre, via
     * la sua dummy read-end in open_fifo_rw). order.sh ha gia' verificato che il
     * warehouse sia vivo; l'alarm copre il caso limite di una FIFO orfana.
     * EINTR: se e' il timeout esco, altrimenti riprovo. */
    int ofd = -1;
    while (ofd < 0) {
        ofd = open(ORDERS_FIFO, O_WRONLY);
        if (ofd < 0) {
            if (errno == EINTR) {
                if (g_timed_out) {
                    fprintf(stderr, "[ORDER] timeout in apertura ORDERS_FIFO "
                                    "(warehouse non risponde)\n");
                    close(rfd); close(wdummy); unlink(resp_path);
                    return ERR_TIMEOUT;
                }
                continue;
            }
            /* ENOENT (FIFO assente) o ENXIO (nessun lettore): warehouse giu'. */
            fprintf(stderr, "[ORDER] open '%s': %s (warehouse attivo?)\n",
                    ORDERS_FIFO, strerror(errno));
            close(rfd); close(wdummy); unlink(resp_path);
            return ERR_WAREHOUSE_DOWN;
        }
    }

    /* ---- 4. componi e invia l'OrderRequest ---- */
    OrderRequest req;
    memset(&req, 0, sizeof(req));               /* azzera padding e code stringhe */
    /* snprintf NUL-termina sempre: i campi restano stringhe valide lato warehouse */
    snprintf(req.client_id, sizeof(req.client_id), "%s", client_id);
    snprintf(req.resp_fifo, sizeof(req.resp_fifo), "%s", resp_path);
    req.item_id  = item_id;
    req.quantity = quantity;

    if (write_all(ofd, &req, sizeof(req)) < 0) {
        if (errno == EPIPE || g_pipe) {         /* warehouse morto mentre scrivevamo */
            fprintf(stderr, "[ORDER] warehouse terminato durante l'invio\n");
            close(ofd); close(rfd); close(wdummy); unlink(resp_path);
            return ERR_WAREHOUSE_DOWN;
        }
        fprintf(stderr, "[ORDER] write su ORDERS_FIFO: %s\n", strerror(errno));
        close(ofd); close(rfd); close(wdummy); unlink(resp_path);
        return ERR_IO;
    }
    close(ofd);                                 /* non serve piu' scrivere */

    /* ---- 5. attendi la risposta sulla FIFO privata ---- */
    OrderResponse resp;
    memset(&resp, 0, sizeof(resp));
    ssize_t n = read_response(rfd, &resp, sizeof(resp));
    alarm(0);                                   /* disarma il timeout */

    close(rfd);
    close(wdummy);
    unlink(resp_path);                          /* la resp_fifo e' nostra: via */

    if (n != (ssize_t)sizeof(resp)) {
        if (g_timed_out) {
            fprintf(stderr, "[ORDER] timeout: nessuna risposta dal warehouse "
                            "entro %d s\n", RESP_TIMEOUT_SECONDS);
            return ERR_TIMEOUT;
        }
        fprintf(stderr, "[ORDER] risposta assente o troncata (%zd byte)\n", n);
        return ERR_IO;
    }

    /* ---- 6. output leggibile + propaga lo status come exit code (spec 2.2.9) ---- */
    print_outcome(client_id, &resp);
    return resp.status;
}