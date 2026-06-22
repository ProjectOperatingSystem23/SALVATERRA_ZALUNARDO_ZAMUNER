/* ============================================================================
 * order_helper.c  --  Helper C dello script order.sh (Project 2026-3)
 *
 * Uso (invocato da order.sh, NON direttamente dall'utente finale):
 *   ./order_helper <client_id> <item_id> <quantity>
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
 *   - order_helper: SOLO la sicurezza del wire-format (client_id non vuoto e che
 *                   entri nel campo della struct). Per il resto e' un trasporto
 *                   "prudente ma fidato": item_id/quantity passano cosi' come
 *                   sono al warehouse, che e' l'autorita' semantica.
 *   - warehouse   : verita' su quantita' (<=0 -> ERR_INVALID_QTY), esistenza
 *                   item (-> ERR_ITEM_NOT_FOUND), stock (-> ERR_OUT_OF_STOCK /
 *                   ERR_PARTIAL_FILL). Tenere qui questi controlli e' DIFESA IN
 *                   PROFONDITA' per chi chiamasse order_helper direttamente.
 *
 * PROTOCOLLO FIFO PRIVATA -- il punto delicato (DEVE combaciare con warehouse.c):
 *   Il warehouse, in send_response(), apre la resp_fifo in O_WRONLY|O_NONBLOCK:
 *   se in quel momento NON c'e' gia' un lettore, la open fallisce (ENXIO) e la
 *   risposta va persa. Percio' order_helper DEVE avere il lato lettura della
 *   resp_fifo gia' aperto PRIMA di inviare l'OrderRequest, e tenerlo aperto fino
 *   alla lettura della risposta. Usiamo lo STESSO schema di open_fifo_r_dw del
 *   warehouse:
 *       read-end O_NONBLOCK  -> la open non blocca anche senza writer
 *       write-end "dummy"    -> c'e' SEMPRE >=1 writer: niente EOF spurio nella
 *                               finestra (picker/packer dormono 1-3s, spec 2.2.4)
 *                               prima che il warehouse risponda
 *       fcntl(F_SETFL)       -> toglie O_NONBLOCK: la read finale e' bloccante
 *   NB: fcntl(F_GETFL/F_SETFL) NON compare nei lab (li' c'e' solo
 *   #include <fcntl.h> per i flag di open): e' una deviazione DICHIARATA, identica
 *   a quella che il warehouse usa in open_fifo_r_dw. La teniamo per coerenza interna
 *   col progetto (da segnalare nel report).
 *
* SEGNALI (Lab03):
 *   - SIGALRM: timeout di sicurezza, via sigaction in setup_handler. Se il
 *     warehouse muore DOPO aver accettato la richiesta, la read sulla resp_fifo
 *     non vedrebbe mai EOF (per via della dummy write-end): l'alarm interrompe la
 *     syscall (EINTR) e usciamo con ERR_TIMEOUT. Handler SENZA SA_RESTART,
 *     altrimenti la read verrebbe riavviata e il timeout sarebbe inefficace.
 *   - SIGPIPE: IGNORATO con SIG_IGN, come nel warehouse. Il default ucciderebbe
 *     il processo; ignorandolo, se il warehouse muore mentre scriviamo la
 *     richiesta la write ritorna -1 con errno=EPIPE (gestito -> ERR_WAREHOUSE_DOWN)
 *     invece di terminarci. Qui NON serve un handler con flag: non c'e' un loop da
 *     interrompere, basta controllare il valore di ritorno della write.
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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <errno.h>
#include "common.h"

/* Attesa massima (secondi) per la risposta del warehouse: rete di sicurezza per
 * un warehouse morto a meta'. Generosa perche' sotto carico l'ordine puo' restare
 * in coda (pending -> picker 1-3s -> packaging -> packer 1-3s, spec 2.2.4/2.2.7). */
#define RESP_TIMEOUT_SECONDS  30

/* ====== Flag settati dagli handler (Lab03): solo sig_atomic_t volatile ====== */
static volatile sig_atomic_t timed_out_flag = 0;   /* alzato da SIGALRM            */

static void on_alarm(int sig) { (void)sig; timed_out_flag = 1; }

/* ====== I/O di basso livello (Lab05) ======================================= */


/* read "completa" della risposta, SENSIBILE AL TIMEOUT. A differenza della
 * read_all del warehouse, su EINTR NON riprendiamo ciecamente: se l'interruzione
 * e' dovuta a SIGALRM (timed_out_flag) abbandoniamo, cosi' il timeout e' effettivo.
 * Ritorna i byte letti, 0 = EOF (non atteso: teniamo la dummy write), -1 = errore. */
static ssize_t read_response(int fd, void *buf, size_t len)
{
    size_t done = 0;
    while (done < len) {
        ssize_t n = read(fd, (char *)buf + done, len - done);
        if (n < 0) {
            if (errno == EINTR) {
                if (timed_out_flag) return -1;   /* timeout scaduto: molla        */
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
        printf("[OK] %s: orders complete item=%d  shipped=%d/%d\n",
               client_id, r->item_id, r->qty_shipped, r->qty_requested);
        break;
    case ERR_PARTIAL_FILL:
        printf("[PARTIAL] %s: item=%d  shipped=%d/%d  (rejected=%d, insufficient stock)\n",
               client_id, r->item_id, r->qty_shipped, r->qty_requested, r->qty_rejected);
        break;
    case ERR_ITEM_NOT_FOUND:
        printf("[REJECTED] %s: item=%d does not exist in the inventory\n",
               client_id, r->item_id);
        break;
    case ERR_OUT_OF_STOCK:
        printf("[REJECTED] %s: item=%d is out of stock\n",
               client_id, r->item_id);
        break;
    case ERR_INVALID_QTY:
        printf("[REJECTED] %s: invalid quantity (%d)\n",
               client_id, r->qty_requested);
        break;
    default:
        printf("[REJECTED] %s: item=%d  status=%d\n",
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
        fprintf(stderr, "[ORDER] client_id missing or too long (max %d)\n",
                MAX_CLIENT_ID - 1);
        return ERR_USAGE;
    }

    /* Segnali (Lab03): timeout + sopravvivenza a un warehouse che muore. */
    setup_handler(SIGALRM, on_alarm);
    setup_handler(SIGPIPE, SIG_IGN); /*l'unica cosa che FORSE non servirebbe a sig ign, sono i sa.sa_flags = 0*/

    /* ---- 1. FIFO di risposta privata: /tmp/order_resp_<PID> ---- */
    char resp_path[MAX_RESP_FIFO];
    snprintf(resp_path, sizeof(resp_path), RESP_FIFO_TEMPLATE, (int)getpid());

    int resp_fd, resp_dummy_w_fd;
    if (open_fifo_r_dw(resp_path, 0600, &resp_fd, &resp_dummy_w_fd) != 0) {
        fprintf(stderr, "[ORDER] init resp_fifo '%s': %s\n", resp_path, strerror(errno));
        unlink(resp_path);              /* se mkfifo l'aveva creata, la rimuoviamo */
        return ERR_IO;
    }

    /* Da qui armiamo il timeout GLOBALE: copre open(ORDERS_FIFO), write e read. */
    alarm(RESP_TIMEOUT_SECONDS);

    /* ---- 3. apri la ORDERS_FIFO in scrittura (bloccante) ----
     * Aspetta che il warehouse abbia il lato lettura aperto (lo tiene sempre, via
     * la sua dummy read-end in open_fifo_r_dw). order.sh ha gia' verificato che il
     * warehouse sia vivo; l'alarm copre il caso limite di una FIFO orfana.
     * EINTR: se e' il timeout esco, altrimenti riprovo. */
    int ofd = -1;
    while (ofd < 0) {
        ofd = open(ORDERS_FIFO, O_WRONLY);
        if (ofd < 0) {
            if (errno == EINTR) {
                if (timed_out_flag) {
                    fprintf(stderr, "[ORDER] timeout while opening ORDERS_FIFO "
                                    "(warehouse is not responding)\n");
                    close(resp_fd); close(resp_dummy_w_fd); unlink(resp_path);
                    return ERR_TIMEOUT;
                }
                continue;
            }
            /* ENOENT (FIFO assente) o ENXIO (nessun lettore): warehouse giu'. */
            fprintf(stderr, "[ORDER] open '%s': %s (is the warehouse active?)\n",
                    ORDERS_FIFO, strerror(errno));
            close(resp_fd); close(resp_dummy_w_fd); unlink(resp_path);
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
    /*SIGPIPE è sincrono alla write. Non arriva "in sottofondo" quando il peer muore: viene generato dalla write stessa
     *nel momento in cui scrivi su una FIFO senza lettori. Quindi non c'è niente da intercettare in modo asincrono: la
     *write ti restituisce già -1 con errno == EPIPE esattamente dove e quando serve. Un handler che alza un flag non
     *aggiunge nessuna informazione che errno==EPIPE non ti dia già.*/
    if (write_all(ofd, &req, sizeof(req)) < 0) {
        fprintf(stderr, "[ORDER] write to ORDERS_FIFO: %s\n", strerror(errno));
        close(ofd); close(resp_fd); close(resp_dummy_w_fd); unlink(resp_path);
        return ERR_IO;
    }
    close(ofd);                                 /* non serve piu' scrivere */

    /* ---- 5. attendi la risposta sulla FIFO privata ---- */
    OrderResponse resp;
    memset(&resp, 0, sizeof(resp));
    ssize_t n = read_response(resp_fd, &resp, sizeof(resp));
    alarm(0);                                   /* disarma il timeout */

    close(resp_fd);
    close(resp_dummy_w_fd);
    unlink(resp_path);                          /* la resp_fifo e' nostra: via */

    if (n != (ssize_t)sizeof(resp)) {
        if (timed_out_flag) {
            fprintf(stderr, "[ORDER] timeout: no response from the warehouse "
                            "within %d s\n", RESP_TIMEOUT_SECONDS);
            return ERR_TIMEOUT;
        }
        fprintf(stderr, "[ORDER] missing or truncated response (%zd byte)\n", n);
        return ERR_IO;
    }

    /* ---- 6. output leggibile + propaga lo status come exit code (spec 2.2.9) ---- */
    print_outcome(client_id, &resp);
    return resp.status;
}
