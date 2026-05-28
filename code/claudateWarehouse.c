/*
 * _POSIX_C_SOURCE 200809L deve stare PRIMA di qualsiasi #include.
 * Abilita le estensioni POSIX.1-2008 usate in questo file:
 *   sigaction, sigsuspend, sigemptyset  → Lab09 Exercise Z.11/Z.12
 */
#define _POSIX_C_SOURCE 200809L

/* ===========================================================================
 * warehouse.c — Fulfillment Center: processo Warehouse
 *
 * Usage:
 *   ./warehouse <num_receivers> <num_pickers> <num_packers>
 *               <queue_capacity> <inventory.csv>
 *
 * ── Pipeline IPC ──────────────────────────────────────────────────────────
 *
 *   order.sh ──write──> ORDERS_FIFO ──read──> [Receiver × N]
 *                                                    │ bq_push
 *                                          [g_pending — BoundedQueue]
 *                                                    │ bq_pop + sleep 1-3s
 *                                          [Picker × N]
 *                                          │ inv_mutex: decrement stock
 *                                          │ bq_push
 *                                          [g_packaging — BoundedQueue]
 *                                                    │ bq_pop + sleep 1-3s
 *                                          [Packer × N]
 *                                          ├─write─> resp_fifo  → order.sh
 *                                          └─write─> orders.log
 *
 *   supplier ──write──> RESTOCK_FIFO ──read──> [Restock × 1]
 *                                              │ inv_mutex: increment stock
 *                                     [Inventory — inv_mutex]
 *
 * ── Segnali ───────────────────────────────────────────────────────────────
 *   SIGTERM/SIGINT → g_shutdown = 1  → main avvia do_shutdown()
 *   SIGUSR1        → g_dump_status=1 → main scrive STATUS_FILE
 *   SIGPIPE        → SIG_IGN         → write su FIFO chiusa non killa il proc
 *
 * ── Shutdown (sentinelle, spec 2.2.10) ────────────────────────────────────
 *   Fase 1: N sentinelle → ORDERS_FIFO  → receiver escono
 *   Fase 2: N sentinelle → g_pending    → picker escono dopo drain
 *   Fase 3: N sentinelle → g_packaging  → packer escono dopo drain
 *   Fase 4: 1 sentinella → RESTOCK_FIFO → restock thread esce
 *
 * ── I/O esclusivamente con file descriptor ────────────────────────────────
 *   Tutti i messaggi di log, debug e risposta usano write() / fd_printf()
 *   su STDOUT_FILENO, STDERR_FILENO o fd espliciti — mai fprintf/perror.
 *   → Lab05 (open/read/write/close, snprintf + write su fd).
 *
 * ── Sezioni ───────────────────────────────────────────────────────────────
 *   1.  Include
 *   2.  Costanti locali
 *   3.  Strutture dati
 *   4.  Variabili globali
 *   5.  Utility I/O              [prototipi → implementazioni]
 *   6.  Inventario               [prototipi → implementazioni]
 *   7.  Bounded Queue            [prototipi → implementazioni]
 *   8.  Logging                  [prototipi → implementazioni]
 *   9.  Thread: Receiver         [prototipo → implementazione]
 *  10.  Thread: Picker           [prototipo → implementazione]
 *  11.  Thread: Packer           [prototipo → implementazione]
 *  12.  Thread: Restock          [prototipo → implementazione]
 *  13.  Signal handlers          [prototipi → implementazioni]
 *  14.  Status dump              [prototipo → implementazione]
 *  15.  Shutdown sequenziale     [prototipo → implementazione]
 *  16.  Main
 * =========================================================================== */


/* ====== 1. INCLUDE ========================================================= */

#include <stdio.h>       /* vsnprintf, snprintf — C standard                */
#include <stdlib.h>      /* atoi, malloc, free, srand, rand, EXIT_FAILURE    */
#include <string.h>      /* memset, memcpy, strncpy, strerror, strlen        */
#include <unistd.h>      /* read, write, close, getpid, sleep, unlink,
                            STDOUT_FILENO, STDERR_FILENO — Lab05             */
#include <fcntl.h>       /* open, O_RDONLY, O_WRONLY, O_CREAT, O_NONBLOCK,
                            O_APPEND, O_TRUNC, fcntl, F_GETFL, F_SETFL      */
#include <sys/stat.h>    /* mkfifo, mode_t — Lab06                          */
#include <sys/types.h>
#include <pthread.h>     /* pthread_create/join, mutex, condvar — Lab09     */
#include <signal.h>      /* sigaction, sigemptyset, sigsuspend — Lab09      */
#include <time.h>        /* time() — Lab09                                  */
#include <errno.h>       /* errno, EINTR — Lab05                            */
#include <stdarg.h>      /* va_list — C standard, usato in fd_printf        */

#include "common.h"      /* ORDERS_FIFO, RESTOCK_FIFO, LOG_FILE, STATUS_FILE,
                            ERR_*, OrderRequest, OrderResponse, RestockMsg,
                            MAX_CLIENT_ID, MAX_DESC, MAX_CATEGORY, MAX_RESP_FIFO,
                            SENTINEL_ITEM_ID, SENTINEL_SUPPLIER_ID,
                            WAREHOUSE_PID_FILE                              */


/* ====== 2. COSTANTI LOCALI ================================================= */

#define MAX_ITEMS  256   /* dimensione array statico items[] nell'inventario */


/* ====== 3. STRUTTURE DATI ================================================== */

/* Stato interno di un ordine lungo la pipeline */


/* Un singolo item dell'inventario */


/*
 * Inventario condiviso tra Picker (decrement) e Restock (increment).
 * Un mutex singolo protegge tutti gli accessi a items[].stock.
 * Scelta mutex: operazioni di stock brevi (un intero), contesa bassa,
 * mutex più semplice e senza rischio di starvation rispetto a rwlock.
 */

/*
 * Ordine interno che percorre la pipeline Receiver → Picker → Packer.

/*
 * Buffer circolare bounded (spec 2.2.5).
 *
 * Produttore: blocca su not_full se size == capacity  (no busy-wait).
 * Consumatore: blocca su not_empty se size == 0        (no busy-wait).
 * Nessun campo "shutdown": i thread escono riconoscendo la sentinella.
 *
 * Perché while e non if in pthread_cond_wait?
 * I "spurious wakeup" possono risvegliare il thread senza segnale esplicito.
 * Il while ri-verifica la condizione ad ogni risveglio.
 * → Lab09 Exercise Z.13: "Always check condition variables in a while loop,
 *   not if, to handle spurious wake-ups correctly."
 */
typedef struct {
    Order          *buf;
    int             head, tail, size, capacity;
    pthread_mutex_t mutex;
    pthread_cond_t  not_full;
    pthread_cond_t  not_empty;
} BoundedQueue;


/* ====== 4. VARIABILI GLOBALI =============================================== */

/*
 * volatile sig_atomic_t: scrittura nel signal handler e lettura nel main
 * sono atomiche rispetto alla consegna del segnale.
 * → Lab09 Exercise Z.12: "volatile sig_atomic_t is signal-safe"
 */


/*
 * File descriptor delle FIFO — strategia "dummy write-end" (Lab06):
 *   Il warehouse tiene aperto il write-end per evitare che read() restituisca
 *   EOF quando nessun client/supplier è connesso. Con il dummy write-end aperto,
 *   read() blocca correttamente finché qualcuno scrive.
 *   Durante lo shutdown vengono iniettate sentinelle esplicite, non si chiude il fd.
 */
static int g_orders_rfd  = -1;   /* ORDERS_FIFO  — read-end  (receiver)          */
static int g_orders_wfd  = -1;   /* ORDERS_FIFO  — write-end (dummy + sentinelle) */
static int g_restock_rfd = -1;   /* RESTOCK_FIFO — read-end  (restock)           */
static int g_restock_wfd = -1;   /* RESTOCK_FIFO — write-end (dummy + sentinella) */

/*
 * Mutex sulla lettura da ORDERS_FIFO.
 * Con N receiver sulla stessa FIFO, senza mutex due thread potrebbero
 * dividersi i byte di un singolo OrderRequest.
 * (La garanzia PIPE_BUF riguarda i writer, non i reader.)
 */


static int             g_log_fd    = -1;



/* ====== 5. UTILITY I/O CON FILE DESCRIPTOR ================================= */

/* ── 5a. PROTOTIPI ─────────────────────────────────────────────────────── */

static void    fd_printf    (int fd, const char *fmt, ...);
static void    rand_sleep_1_3(void);

/* ── 5b. IMPLEMENTAZIONI ───────────────────────────────────────────────── */

/*
 * Legge esattamente 'len' byte da fd, gestendo EINTR e short-read.
 * Ritorna: byte letti (< len solo su EOF), -1 su errore non-EINTR.
 *
 * Perché serve: read() può restituire meno byte del richiesto (short read).
 * Per struct binarie (OrderRequest, RestockMsg) occorre leggere esattamente
 * sizeof(struct) byte, altrimenti i campi vengono interpretati in modo errato.
 * → Lab05 (read su file descriptor, gestione EINTR).
 */
static ssize_t read_all(int fd, void *buf, size_t len)
{
    size_t done = 0;
    while (done < len) {
        ssize_t n = read(fd, (char *)buf + done, len - done);
        if (n < 0) {
            if (errno == EINTR) continue;   /* segnale arrivato: riprova   */
            return -1;
        }
        if (n == 0) break;                  /* EOF                         */
        done += (size_t)n;
    }
    return (ssize_t)done;
}

/*
 * Scrive esattamente 'len' byte su fd, gestendo EINTR e short-write.
 * Ritorna: byte scritti (uguale a len se ok), -1 su errore.
 * → Lab05 (write su file descriptor, gestione EINTR).
 */
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

/*
 * Legge una riga da fd (fino a '\n' o EOF), NUL-terminata.
 * Lettura byte per byte: semplice e corretta per file CSV piccoli
 * letti una sola volta al startup.
 * → Lab05 (open/read con file descriptor).
 */

static ssize_t fd_read_line(int fd, char *buf, size_t buf_size)
{
    size_t i = 0;
    while (i < buf_size - 1) {
        char    c;
        ssize_t n = read(fd, &c, 1);
        if (n < 0) { if (errno == EINTR) continue; return -1; }
        if (n == 0) break;           /* EOF                                */
        if (c == '\n') break;
        if (c == '\r') continue;     /* ignora CR (file Windows)           */
        buf[i++] = c;
    }
    buf[i] = '\0';
    return (ssize_t)i;
}

/*
 * Scrive una stringa formattata sul file descriptor fd.
 * Pattern Lab05 Exercise I.3: snprintf(buf, sizeof(buf), fmt, args) → write(fd, buf, n).
 * vsnprintf è la versione variadica di snprintf (standard C, non POSIX).
 * Non usa FILE* (fprintf): lavora esclusivamente con file descriptor.
 */
static void fd_printf(int fd, const char *fmt, ...)
{
    char    buf[1024];
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);
    if (n > 0) write_all(fd, buf, (size_t)n);
}

/*
 * Sleep casuale in [1, 3] secondi — simula lavoro fisico (spec 2.2.7).
 * sleep() — Lab09 (usato negli esercizi sulle pause tra iterazioni).
 */
static void rand_sleep_1_3(void)
{
    sleep(1 + (unsigned)rand() % 3);
}


/* ====== 6. INVENTARIO ====================================================== */

/* ── 6a. PROTOTIPI ─────────────────────────────────────────────────────── */


/*TODO: COSA CAZZO FA QUESTA FUNZIONE*/
static Item *inventory_find_locked     (Inventory *inv, int item_id);



/* ── 6b. IMPLEMENTAZIONI ───────────────────────────────────────────────── */

/*
 * Estrae un campo CSV (gestisce campi quotati tipo "Wireless Mouse").
 * Avanza *pp oltre il campo e il separatore ','.
 */
static void csv_parse_field(char **pp, char *dst, int dst_size)
{
    char *p = *pp;
    int   i = 0;
    if (*p == '"') {
        p++;
        while (*p && *p != '"') { if (i < dst_size - 1) dst[i++] = *p; p++; }
        if (*p == '"') p++;
    } else {
        while (*p && *p != ',' && *p != '\n' && *p != '\r') {
            if (i < dst_size - 1) dst[i++] = *p;
            p++;
        }
    }
    dst[i] = '\0';
    if (*p == ',') p++;
    *pp = p;
}

/*
 * Carica l'inventario dal CSV con file descriptor — Lab05 (open/read/close).
 * Formato: ItemID,Description,Category,Stock (header + righe dati).
 * Tutti i messaggi di errore su STDERR_FILENO via fd_printf — Lab05.
 * Ritorna ERR_OK o ERR_IO.
 */
static int inventory_load(Inventory *inv, const char *path)
{
    int fd = open(path, O_RDONLY);   /* open() — Lab05 */
    if (fd < 0) {
        fd_printf(STDERR_FILENO, "[WAREHOUSE] open '%s': %s\n",
                  path, strerror(errno));
        return ERR_IO;
    }

    char line[512];
    /* Salta la riga header */
    if (fd_read_line(fd, line, sizeof(line)) <= 0) {
        fd_printf(STDERR_FILENO, "[WAREHOUSE] Inventory vuoto o illeggibile.\n");
        close(fd);
        return ERR_IO;
    }

    inv->count = 0;
    while (fd_read_line(fd, line, sizeof(line)) > 0) {
        if (line[0] == '\0') continue;
        if (inv->count >= MAX_ITEMS) {
            fd_printf(STDERR_FILENO,
                      "[WAREHOUSE] Warning: inventory > MAX_ITEMS=%d, troncato.\n",
                      MAX_ITEMS);
            break;
        }
        Item *it = &inv->items[inv->count];
        char *p  = line;
        char  tmp[32];

        csv_parse_field(&p, tmp,             sizeof(tmp));
        it->item_id = atoi(tmp);
        if (it->item_id <= 0) continue;   /* salta righe con ItemID <= 0  */

        csv_parse_field(&p, it->description, sizeof(it->description));
        csv_parse_field(&p, it->category,    sizeof(it->category));
        csv_parse_field(&p, tmp,             sizeof(tmp));
        it->stock = atoi(tmp);

        inv->count++;
    }

    close(fd);   /* chiudiamo sempre il fd — Lab05 */
    return ERR_OK;
}

/*
 * Cerca un item per item_id. Ritorna puntatore o NULL.
 * DEVE essere chiamata con inv->mutex già acquisito dal chiamante.
 */
static Item *inventory_find_locked(Inventory *inv, int item_id)
{
    for (int i = 0; i < inv->count; i++)
        if (inv->items[i].item_id == item_id)
            return &inv->items[i];
    return NULL;
}

/*
 * Decrementa stock di 'qty' unità. Gestisce consegna parziale (spec 2.2.2):
 * se stock < qty, spedisce quanto disponibile e setta ERR_PARTIAL.
 * Ritorna unità spedite (0..qty); codice errore scritto in *err_out.
 * DEVE essere chiamata con inv->mutex già acquisito.
 */
static int inventory_decrement_locked(Inventory *inv, int item_id,
                                      int qty, int *err_out)
{
    Item *it = inventory_find_locked(inv, item_id);
    if (!it)            { *err_out = ERR_ITEM_NOT_FOUND; return 0; }
    if (it->stock <= 0) { *err_out = ERR_OUT_OF_STOCK;   return 0; }

    int ship = (it->stock >= qty) ? qty : it->stock;
    it->stock -= ship;
    *err_out = (ship < qty) ? ERR_PARTIAL : ERR_OK;
    return ship;
}

/*
 * Incrementa stock (chiamato dal restock thread).
 * Acquisisce e rilascia inv->mutex internamente — stessa lock dei Picker.
 */
static void inventory_increment(Inventory *inv, int item_id, int qty)
{
    pthread_mutex_lock(&inv->mutex);
    Item *it = inventory_find_locked(inv, item_id);
    if (it) {
        it->stock += qty;
        fd_printf(STDOUT_FILENO, "[RESTOCK] item_id=%d +%d → stock=%d\n",
                  item_id, qty, it->stock);
    } else {
        fd_printf(STDERR_FILENO, "[RESTOCK] Warning: item_id=%d non trovato.\n",
                  item_id);
    }
    pthread_mutex_unlock(&inv->mutex);
}


/* ====== 7. BOUNDED QUEUE ================================================== */

/* ── 7a. PROTOTIPI ─────────────────────────────────────────────────────── */


/* ── 7b. IMPLEMENTAZIONI ───────────────────────────────────────────────── */

/*
 * Inizializza il buffer circolare con capacità 'capacity' (nota a runtime).
 * Ritorna ERR_OK o ERR_IO.
 */
static int bq_init(BoundedQueue *q, int capacity)
{
    q->buf = malloc((size_t)capacity * sizeof(Order));
    if (!q->buf) return ERR_IO;

    q->head = q->tail = q->size = 0;
    q->capacity = capacity;

    if (pthread_mutex_init(&q->mutex,     NULL) != 0) { free(q->buf); return ERR_IO; }
    if (pthread_cond_init (&q->not_full,  NULL) != 0) { free(q->buf); return ERR_IO; }
    if (pthread_cond_init (&q->not_empty, NULL) != 0) { free(q->buf); return ERR_IO; }
    return ERR_OK;
}

/* Dealloca risorse (chiamata dopo il join di tutti i thread). */
static void bq_destroy(BoundedQueue *q)
{
    pthread_cond_destroy (&q->not_empty);
    pthread_cond_destroy (&q->not_full);
    pthread_mutex_destroy(&q->mutex);
    free(q->buf);
    q->buf = NULL;
}

/*
 * Inserisce un Order (produttore). BLOCCA se la coda è piena.
 * Il produttore non esce per causa della coda, ma leggendo la sua sentinella.
 *
 * → Lab09 Exercise Z.13 (producer):
 *     pthread_mutex_lock(&m);
 *     while (count == NBUF)           ← while, non if: spurious wakeup
 *         pthread_cond_wait(&not_full, &m);
 *     ... inserisci ...
 *     pthread_cond_signal(&not_empty);
 *     pthread_mutex_unlock(&m);
 */
static void bq_push(BoundedQueue *q, const Order *order)
{
    pthread_mutex_lock(&q->mutex);
    while (q->size == q->capacity)
        pthread_cond_wait(&q->not_full, &q->mutex);

    q->buf[q->tail] = *order;
    q->tail = (q->tail + 1) % q->capacity;
    q->size++;

    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->mutex);
}

/*
 * Estrae un Order (consumatore). BLOCCA se la coda è vuota.
 * Il consumatore esce quando estrae un Order con item_id == SENTINEL_ITEM_ID.
 *
 * → Lab09 Exercise Z.13 (consumer):
 *     pthread_mutex_lock(&m);
 *     while (count == 0)
 *         pthread_cond_wait(&not_empty, &m);
 *     ... estrai ...
 *     pthread_cond_signal(&not_full);
 *     pthread_mutex_unlock(&m);
 */
static void bq_pop(BoundedQueue *q, Order *out)
{
    pthread_mutex_lock(&q->mutex);
    while (q->size == 0)
        pthread_cond_wait(&q->not_empty, &q->mutex);

    *out    = q->buf[q->head];
    q->head = (q->head + 1) % q->capacity;
    q->size--;

    pthread_cond_signal(&q->not_full);
    pthread_mutex_unlock(&q->mutex);
}


/* ====== 8. LOGGING ========================================================= */

/* ── 8a. PROTOTIPI ─────────────────────────────────────────────────────── */
/*TODO: CAPIRE SE SERVE*/

//static void send_response(const char *resp_fifo, int err_code,
                          int qty_shipped, int qty_rejected);

/* ── 8b. IMPLEMENTAZIONI ───────────────────────────────────────────────── */

/*
 * Scrive una riga su orders.log (g_log_fd aperto in append in main).
 * Formato: unix_ts|client_id|item_id|qty_req|qty_shipped|status|err_code
 *
 * Timestamp: time() restituisce Unix time (long) — Lab09.
 * snprintf formatta il timestamp — Lab05 Exercise I.3.
 * g_log_mutex: protegge g_log_fd da scritture interleaved dei packer.
 * fd_printf su g_log_fd: scrive con write() — Lab05.
 */
static void log_order(const Order *o)
{
    char   tbuf[32];
    time_t now = time(NULL);
    snprintf(tbuf, sizeof(tbuf), "%ld", (long)now);

    const char *st;
    if      (o->error_code == ERR_PARTIAL) st = "PARTIAL";
    else if (o->status == ORDER_SHIPPED)   st = "SHIPPED";
    else                                   st = "REJECTED";

    pthread_mutex_lock(&g_log_mutex);
    if (g_log_fd >= 0)
        fd_printf(g_log_fd, "%s|%s|%d|%d|%d|%s|%d\n",
                  tbuf, o->client_id, o->item_id,
                  o->quantity, o->qty_shipped, st, o->error_code);
    pthread_mutex_unlock(&g_log_mutex);
}

/*
 * Invia OrderResponse alla FIFO privata del client.
 *
 * O_NONBLOCK sull'apertura: se il client ha già chiuso la FIFO (crash,
 * timeout), open() ritorna subito con ENXIO invece di bloccarsi.
 * → Lab06: "Opening a FIFO blocks [...] unless O_NONBLOCK is set".
 * sizeof(OrderResponse) = 12 B < PIPE_BUF → write atomica — Lab06.
 */
static void send_response(const char *resp_fifo, int err_code,
                          int qty_shipped, int qty_rejected)
{
    if (resp_fifo[0] == '\0') return;

    int fd = open(resp_fifo, O_WRONLY | O_NONBLOCK);
    if (fd < 0) {
        fd_printf(STDERR_FILENO, "[WAREHOUSE] open resp_fifo '%s': %s\n",
                  resp_fifo, strerror(errno));
        return;
    }
    OrderResponse resp = { err_code, qty_shipped, qty_rejected };
    write_all(fd, &resp, sizeof(resp));
    close(fd);
}




/* ====== 13. SIGNAL HANDLERS ================================================ */

/* ── 13a. PROTOTIPI ────────────────────────────────────────────────────── */



/* ── 13b. IMPLEMENTAZIONI ──────────────────────────────────────────────── */

/*
 * SIGTERM / SIGINT — avvia lo shutdown.
 *
 * Il handler FA SOLO UNA COSA: setta il flag volatile sig_atomic_t.
 * write(), pthread_mutex_lock(), bq_push() non sono async-signal-safe →
 * non vanno mai chiamate da un signal handler.
 * Tutta la logica di shutdown viene eseguita dal main thread.
 *
 * → Lab09 Exercise Z.12: "volatile sig_atomic_t is signal-safe"
 *   on_usr1(int s) { (void)s; got_usr1 = 1; }
 */
static void handle_sigterm(int sig)
{
    (void)sig;
    g_shutdown = 1;
}

/*
 * SIGUSR1 — richiesta dump stato da manage.sh status.
 * Setta il flag: l'I/O (open/write) lo fa il main thread.
 */
static void handle_sigusr1(int sig)
{
    (void)sig;
    g_dump_status = 1;
}


/* ====== 14. STATUS DUMP ==================================================== */

/* ── 14a. PROTOTIPO ────────────────────────────────────────────────────── */

static void do_status_dump(void);

/* ── 14b. IMPLEMENTAZIONE ──────────────────────────────────────────────── */

/*
 * Scrive snapshot di inventario e code su STATUS_FILE.
 * Chiamata SOLO dal main thread (non dal signal handler — Lab09).
 *
 * open(O_WRONLY|O_CREAT|O_TRUNC) + fd_printf su fd — Lab05.
 * Formato: chiave=valore; righe ITEM| parsabili con awk — Lab09.
 * Mutex su g_inv: il dump è concorrente con picker/restock.
 */
static void do_status_dump(void)
{
    int fd = open(STATUS_FILE, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        fd_printf(STDERR_FILENO, "[WAREHOUSE] open STATUS_FILE: %s\n",
                  strerror(errno));
        return;
    }

    fd_printf(fd, "PID=%d\n",               (int)getpid());
    fd_printf(fd, "RECEIVERS=%d\n",         g_num_receivers);
    fd_printf(fd, "PICKERS=%d\n",           g_num_pickers);
    fd_printf(fd, "PACKERS=%d\n",           g_num_packers);
    fd_printf(fd, "PENDING_QUEUE=%d/%d\n",  g_pending.size,   g_pending.capacity);
    fd_printf(fd, "PACKAGING_QUEUE=%d/%d\n",g_packaging.size, g_packaging.capacity);

    pthread_mutex_lock(&g_inv.mutex);
    fd_printf(fd, "INVENTORY_COUNT=%d\n", g_inv.count);
    for (int i = 0; i < g_inv.count; i++) {
        Item *it = &g_inv.items[i];
        fd_printf(fd, "ITEM|%d|%s|%s|%d\n",
                  it->item_id, it->description, it->category, it->stock);
    }
    pthread_mutex_unlock(&g_inv.mutex);

    close(fd);
}


/* ====== 15. SHUTDOWN SEQUENZIALE VIA SENTINELLE ============================ */

/* ── 15a. PROTOTIPO ────────────────────────────────────────────────────── */

static void do_shutdown(pthread_t *recv_th, pthread_t *pick_th,
                        pthread_t *pack_th, pthread_t  rest_th);

/* ── 15b. IMPLEMENTAZIONE ──────────────────────────────────────────────── */

/*
 * Shutdown ordinato che garantisce il drain degli ordini in volo
 * (spec 2.2.10: "complete or cleanly cancel pending orders").
 *
 * Tutte le sentinelle vengono scritte con write_all() su file descriptor
 * (g_orders_wfd, g_restock_wfd) o iniettate con bq_push() — mai dal
 * signal handler, sempre dal main thread.
 *
 *  Fase 1 — Stop ingresso nuovi ordini:
 *    N sentinelle → ORDERS_FIFO via write_all(g_orders_wfd).
 *    sizeof(OrderRequest) = 328 B < PIPE_BUF → write atomica — Lab06.
 *    Ogni receiver legge la sua sentinella ed esce.
 *
 *  Fase 2 — Join receiver + drain g_pending:
 *    N_pickers sentinelle via bq_push(&g_pending).
 *    bq_push può bloccarsi se g_pending è piena: ok, i picker continuano
 *    a drenaree si libera spazio → nessun deadlock.
 *
 *  Fase 3 — Join picker + drain g_packaging:
 *    N_packers sentinelle via bq_push(&g_packaging).
 *    Ogni packer invia risposta al client e scrive log prima di uscire.
 *
 *  Fase 4 — Join packer + stop restock:
 *    1 sentinella RestockMsg(supplier_id=-1) via write_all(g_restock_wfd).
 */
static void do_shutdown(pthread_t *recv_th, pthread_t *pick_th,
                        pthread_t *pack_th, pthread_t  rest_th)
{
    fd_printf(STDOUT_FILENO, "[WAREHOUSE] Shutdown: inizio procedura...\n");

    /* ── Fase 1: sentinelle per i receiver ─────────────────────────────── */
    {
        OrderRequest sentinel;
        memset(&sentinel, 0, sizeof(sentinel));
        sentinel.item_id = SENTINEL_ITEM_ID;   /* -1 */

        for (int i = 0; i < g_num_receivers; i++) {
            if (write_all(g_orders_wfd, &sentinel, sizeof(sentinel)) < 0)
                fd_printf(STDERR_FILENO,
                          "[SHUTDOWN] write sentinel ORDERS_FIFO: %s\n",
                          strerror(errno));
        }
        fd_printf(STDOUT_FILENO,
                  "[WAREHOUSE] Shutdown: %d sentinelle → ORDERS_FIFO\n",
                  g_num_receivers);
    }

    for (int i = 0; i < g_num_receivers; i++) pthread_join(recv_th[i], NULL);
    fd_printf(STDOUT_FILENO, "[WAREHOUSE] Shutdown: receiver terminati.\n");

    /* ── Fase 2: sentinelle per i picker ────────────────────────────────── */
    {
        Order sentinel;
        memset(&sentinel, 0, sizeof(sentinel));
        sentinel.item_id = SENTINEL_ITEM_ID;

        for (int i = 0; i < g_num_pickers; i++)
            bq_push(&g_pending, &sentinel);

        fd_printf(STDOUT_FILENO,
                  "[WAREHOUSE] Shutdown: %d sentinelle → g_pending\n",
                  g_num_pickers);
    }

    for (int i = 0; i < g_num_pickers; i++) pthread_join(pick_th[i], NULL);
    fd_printf(STDOUT_FILENO, "[WAREHOUSE] Shutdown: picker terminati.\n");

    /* ── Fase 3: sentinelle per i packer ────────────────────────────────── */
    {
        Order sentinel;
        memset(&sentinel, 0, sizeof(sentinel));
        sentinel.item_id = SENTINEL_ITEM_ID;

        for (int i = 0; i < g_num_packers; i++)
            bq_push(&g_packaging, &sentinel);

        fd_printf(STDOUT_FILENO,
                  "[WAREHOUSE] Shutdown: %d sentinelle → g_packaging\n",
                  g_num_packers);
    }

    for (int i = 0; i < g_num_packers; i++) pthread_join(pack_th[i], NULL);
    fd_printf(STDOUT_FILENO, "[WAREHOUSE] Shutdown: packer terminati.\n");

    /* ── Fase 4: sentinella per il restock thread ───────────────────────── */
    {
        RestockMsg sentinel = { SENTINEL_SUPPLIER_ID, 0, 0 };
        if (write_all(g_restock_wfd, &sentinel, sizeof(sentinel)) < 0)
            fd_printf(STDERR_FILENO,
                      "[SHUTDOWN] write sentinel RESTOCK_FIFO: %s\n",
                      strerror(errno));
    }

    pthread_join(rest_th, NULL);
    fd_printf(STDOUT_FILENO, "[WAREHOUSE] Shutdown: restock thread terminato.\n");
}


/* ====== 16. MAIN =========================================================== */

int main(int argc, char *argv[])
{
    /* ── Validazione argomenti ──────────────────────────────────────────── */





    /* ── Carica inventario ──────────────────────────────────────────────── */


    fd_printf(STDOUT_FILENO, "[WAREHOUSE] Inventario: %d item da '%s'.\n",
              g_inv.count, inv_path);

    /* ── Apri log file con file descriptor ─────────────────────────────── */
    /*
     * O_APPEND: ogni write() va in fondo al file — sicuro con packer
     *   concorrenti (ogni write atomica su O_APPEND è garantita dal kernel).
     * O_CREAT: crea il file se non esiste (permessi 0644 = rw-r--r--).
     * open() + write() invece di fopen() + fprintf() — Lab05.
     */
    g_log_fd = open(LOG_FILE, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (g_log_fd < 0) {
        fd_printf(STDERR_FILENO, "[WAREHOUSE] open log '%s': %s\n",
                  LOG_FILE, strerror(errno));
        return EXIT_FAILURE;
    }

    /* ── Inizializza bounded queues ─────────────────────────────────────── */
    if (bq_init(&g_pending,   queue_cap) != ERR_OK ||
        bq_init(&g_packaging, queue_cap) != ERR_OK) {
        fd_printf(STDERR_FILENO,
                  "[WAREHOUSE] Impossibile inizializzare le code.\n");
        return EXIT_FAILURE;
    }

    /* ── Apri FIFO con strategia dummy write-end (Lab06) ────────────────── */
    /*
     * open() su FIFO in O_RDONLY bloccante attende che qualcuno apra il
     * lato O_WRONLY (man 7 fifo). Soluzione in tre passi — Lab06:
     *   1. open read-end  con O_NONBLOCK → ritorna subito
     *   2. open write-end con O_NONBLOCK → non attende il lettore
     *   3. rimuovi O_NONBLOCK dal read-end con fcntl(F_SETFL) → bloccante
     * Il write-end resta aperto (dummy) per impedire EOF spurii — Lab06.
     */

    /* ORDERS_FIFO */
    g_orders_rfd = open(ORDERS_FIFO, O_RDONLY | O_NONBLOCK);
    if (g_orders_rfd < 0) {
        fd_printf(STDERR_FILENO, "[WAREHOUSE] open ORDERS_FIFO rfd: %s\n",
                  strerror(errno));
        return EXIT_FAILURE;
    }
    g_orders_wfd = open(ORDERS_FIFO, O_WRONLY | O_NONBLOCK);
    if (g_orders_wfd < 0) {
        fd_printf(STDERR_FILENO, "[WAREHOUSE] open ORDERS_FIFO wfd: %s\n",
                  strerror(errno));
        return EXIT_FAILURE;
    }
    fcntl(g_orders_rfd, F_SETFL,
          fcntl(g_orders_rfd, F_GETFL, 0) & ~O_NONBLOCK);

    /* RESTOCK_FIFO */
    g_restock_rfd = open(RESTOCK_FIFO, O_RDONLY | O_NONBLOCK);
    if (g_restock_rfd < 0) {
        fd_printf(STDERR_FILENO, "[WAREHOUSE] open RESTOCK_FIFO rfd: %s\n",
                  strerror(errno));
        return EXIT_FAILURE;
    }
    g_restock_wfd = open(RESTOCK_FIFO, O_WRONLY | O_NONBLOCK);
    if (g_restock_wfd < 0) {
        fd_printf(STDERR_FILENO, "[WAREHOUSE] open RESTOCK_FIFO wfd: %s\n",
                  strerror(errno));
        return EXIT_FAILURE;
    }
    fcntl(g_restock_rfd, F_SETFL,
          fcntl(g_restock_rfd, F_GETFL, 0) & ~O_NONBLOCK);

    fd_printf(STDOUT_FILENO, "[WAREHOUSE] FIFO aperte: %s, %s\n",
              ORDERS_FIFO, RESTOCK_FIFO);

    /* ── Installa signal handler (Lab09 Z.11/Z.12) ──────────────────────── */
    /*
     * sigaction() → comportamento portabile e prevedibile.
     * → Lab09 Exercise Z.12:
     *     struct sigaction sa = {0};
     *     sa.sa_handler = handler;
     *     sigaction(SIG, &sa, NULL);
     * sa_flags = 0: sigsuspend() ritorna non appena un segnale arriva.
     */
    {
        struct sigaction sa;
        memset(&sa, 0, sizeof(sa));
        sigemptyset(&sa.sa_mask);

        sa.sa_handler = handle_sigterm;
        sigaction(SIGTERM, &sa, NULL);
        sigaction(SIGINT,  &sa, NULL);

        sa.sa_handler = handle_sigusr1;
        sigaction(SIGUSR1, &sa, NULL);

        /* SIGPIPE ignorato: write su FIFO chiusa ritorna -1/EPIPE invece
         * di terminare il processo — gestito in send_response().          */
        sa.sa_handler = SIG_IGN;
        sigaction(SIGPIPE, &sa, NULL);
    }

    /* ── Lancia i thread pool ───────────────────────────────────────────── */
    /*
     * malloc + memset: alloca e azzera array di pthread_t a runtime.
     * pthread_create — Lab09 Exercise Z.13 / Lab05.
     */
    pthread_t *recv_th = malloc((size_t)g_num_receivers * sizeof(pthread_t));
    pthread_t *pick_th = malloc((size_t)g_num_pickers   * sizeof(pthread_t));
    pthread_t *pack_th = malloc((size_t)g_num_packers   * sizeof(pthread_t));
    pthread_t  rest_th;

    if (!recv_th || !pick_th || !pack_th) {
        fd_printf(STDERR_FILENO, "[WAREHOUSE] malloc thread arrays: %s\n",
                  strerror(errno));
        return EXIT_FAILURE;
    }
    memset(recv_th, 0, (size_t)g_num_receivers * sizeof(pthread_t));
    memset(pick_th, 0, (size_t)g_num_pickers   * sizeof(pthread_t));
    memset(pack_th, 0, (size_t)g_num_packers   * sizeof(pthread_t));

    for (int i = 0; i < g_num_receivers; i++)
        pthread_create(&recv_th[i], NULL, thread_receiver, NULL);
    for (int i = 0; i < g_num_pickers; i++)
        pthread_create(&pick_th[i], NULL, thread_picker,   NULL);
    for (int i = 0; i < g_num_packers; i++)
        pthread_create(&pack_th[i], NULL, thread_packer,   NULL);
    pthread_create(&rest_th, NULL, thread_restock, NULL);

    fd_printf(STDOUT_FILENO,
        "[WAREHOUSE] Avviato — receivers=%d pickers=%d packers=%d"
        " queue_cap=%d PID=%d\n",
        g_num_receivers, g_num_pickers, g_num_packers,
        queue_cap, (int)getpid());

    /* ── Loop principale: attende segnali con sigsuspend ────────────────── */
    /*
     * sigsuspend(&empty_mask): sblocca atomicamente tutti i segnali e
     * sospende il processo fino all'arrivo di uno.
     * → Lab09 Exercise Z.12: "sigsuspend(&empty_set) atomically unblocks
     *   all signals and sleeps until one arrives."
     *
     *   SIGUSR1 → g_dump_status=1 → sigsuspend ritorna → dump → riprende
     *   SIGTERM → g_shutdown=1    → sigsuspend ritorna → esce dal while
     */
    {
        sigset_t empty_mask;
        sigemptyset(&empty_mask);   /* maschera vuota — Lab09 Z.12 */

        while (!g_shutdown) {
            sigsuspend(&empty_mask);

            if (g_dump_status) {
                g_dump_status = 0;
                do_status_dump();
                fd_printf(STDOUT_FILENO,
                          "[WAREHOUSE] Status dump → '%s'\n", STATUS_FILE);
            }
        }
    }

    fd_printf(STDOUT_FILENO,
              "[WAREHOUSE] SIGTERM/SIGINT ricevuto, avvio shutdown.\n");

    /* ── Shutdown sequenziale via sentinelle ────────────────────────────── */
    do_shutdown(recv_th, pick_th, pack_th, rest_th);

    /* ── Pulizia finale ──────────────────────────────────────────────────── */
    if (g_orders_rfd  >= 0) close(g_orders_rfd);
    if (g_orders_wfd  >= 0) close(g_orders_wfd);
    if (g_restock_rfd >= 0) close(g_restock_rfd);
    if (g_restock_wfd >= 0) close(g_restock_wfd);
    if (g_log_fd      >= 0) close(g_log_fd);

    bq_destroy(&g_pending);
    bq_destroy(&g_packaging);
    pthread_mutex_destroy(&g_inv.mutex);
    pthread_mutex_destroy(&g_log_mutex);
    pthread_mutex_destroy(&g_orders_read_mutex);

    free(recv_th);
    free(pick_th);
    free(pack_th);

    unlink(WAREHOUSE_PID_FILE);

    fd_printf(STDOUT_FILENO, "[WAREHOUSE] Shutdown completo.\n");
    return EXIT_SUCCESS;
}
 rest_th);

    /* ── Pulizia finale ──────────────────────────────────────────────────── */
    if (g_orders_rfd  >= 0) close(g_orders_rfd);
    if (g_orders_wfd  >= 0) close(g_orders_wfd);
    if (g_restock_rfd >= 0) close(g_restock_rfd);
    if (g_restock_wfd >= 0) close(g_restock_wfd);
    if (g_log_fd      >= 0) close(g_log_fd);

    bq_destroy(&g_pending);
    bq_destroy(&g_packaging);
    pthread_mutex_destroy(&g_inv.mutex);
    pthread_mutex_destroy(&g_log_mutex);
    pthread_mutex_destroy(&g_orders_read_mutex);

    free(recv_th);
    free(pick_th);
    free(pack_th);

    unlink(WAREHOUSE_PID_FILE);

    fd_printf(STDOUT_FILENO, "[WAREHOUSE] Shutdown completo.\n");
    return EXIT_SUCCESS;
}
/////////////////////////////CHATTATA:////////////////////////////
//GLOBALI:
static Inventory inv;

static volatile sig_atomic_t shutdown_flag = 0;
static volatile sig_atomic_t dump_status_flag = 0;


static pthread_mutex_t restock_read_mutex = PTHREAD_MUTEX_INITIALIZER;



static void signal_handler(int signum)
{
    if (signum == SIGTERM || signum == SIGINT) {
        shutdown_flag = 1;
    } else if (signum == SIGUSR1) {
        dump_status_flag = 1;
    }
}

static void install_signal_handlers(void)
{
    struct sigaction sa;

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;

    if (sigaction(SIGTERM, &sa, NULL) == -1) {
        perror("[warehouse] sigaction SIGTERM");
        exit(EXIT_FAILURE);
    }

    if (sigaction(SIGINT, &sa, NULL) == -1) {
        perror("[warehouse] sigaction SIGINT");
        exit(EXIT_FAILURE);
    }

    if (sigaction(SIGUSR1, &sa, NULL) == -1) {
        perror("[warehouse] sigaction SIGUSR1");
        exit(EXIT_FAILURE);
    }
}
static int write_status_snapshot(void)
{
    int fd;
    char line[512];

    fd = open(STATUS_FILE, O_CREAT | O_TRUNC | O_WRONLY, 0644);
    if (fd == -1) {
        perror("[warehouse] open status file");
        return -1;
    }

    pthread_mutex_lock(&inv.mutex);

    for (int i = 0; i < inv.count; i++) {
        int len = snprintf(line, sizeof(line),
                           "%d,%s,%s,%d\n",
                           inv.items[i].item_id,
                           inv.items[i].description,
                           inv.items[i].category,
                           inv.items[i].stock);

        if (len > 0 && len < (int)sizeof(line)) {
            if (write_full(fd, line, (size_t)len) != len) {
                perror("[warehouse] write status");
                break;
            }
        }
    }

    pthread_mutex_unlock(&inv.mutex);

    close(fd);
    return 0;
}
static void send_order_sentinels(int n)
{
    int fd;
    OrderRequest sentinel;

    memset(&sentinel, 0, sizeof(sentinel));
    sentinel.item_id = -1;
    sentinel.quantity = 0;

    fd = open(ORDERS_FIFO, O_WRONLY | O_NONBLOCK);
    if (fd == -1) {
        perror("[warehouse] open orders fifo for sentinels");
        return;
    }

    for (int i = 0; i < n; i++) {
        if (write_full(fd, &sentinel, sizeof(sentinel)) != (ssize_t)sizeof(sentinel)) {
            perror("[warehouse] write order sentinel");
            break;
        }
    }

    close(fd);
}

static void send_restock_sentinel(void)
{
    int fd;
    RestockRequest sentinel;

    memset(&sentinel, 0, sizeof(sentinel));
    sentinel.item_id = -1;
    sentinel.quantity = 0;

    fd = open(RESTOCK_FIFO, O_WRONLY | O_NONBLOCK);
    if (fd == -1) {
        perror("[warehouse] open restock fifo for sentinel");
        return;
    }

    if (write_full(fd, &sentinel, sizeof(sentinel)) != (ssize_t)sizeof(sentinel)) {
        perror("[warehouse] write restock sentinel");
    }

    close(fd);
}

static void sleep_random_1_to_3(unsigned int *seed)
{
    int seconds = (int)(rand_r(seed) % 3) + 1;
    sleep((unsigned int)seconds);
}

static void *picker_thread_func(void *arg)
{
    PickerArgs *args = arg;
    unsigned int seed = (unsigned int)time(NULL) ^ (unsigned int)(uintptr_t)pthread_self();

    while (!shutdown_flag) {
        Order order;

        if (consume_from_queue(args->pending_orders_q, &order) == -1) {
            break;
        }

        order.status = ORDER_PICKING;
        sleep_random_1_to_3(&seed);

        inventory_fulfill_order(&inv, &order);

        if (order.qty_shipped <= 0) {
            OrderResponse resp;

            build_response(&order, &resp);
            send_response(order.request.resp_fifo, &resp);
            continue;
        }

        order.status = ORDER_PACKING;

        if (produce_in_queue(args->packaging_q, &order) == -1) {
            break;
        }
    }

    return NULL;
}

static void *receiver_thread_func(void *arg)
{
    ReceiverArgs *args = arg;

    while (!shutdown_flag) {
        OrderRequest req;
        Order order;
        int rc;

        rc = read_order_request(args->orders_fd, &req);

        if (rc == 0) {
            continue;
        }

        if (rc < 0) {
            if (shutdown_flag) break;
            fprintf(stderr, "[warehouse] failed to read OrderRequest\n");
            continue;
        }

        if (req.item_id == -1) {
            break;
        }

        memset(&order, 0, sizeof(order));
        order.request = req;
        order.order_id = next_order_id();
        order.status = ORDER_RECEIVED;

        if (req.quantity <= 0) {
            OrderResponse resp;

            order.status = ORDER_REJECTED;
            order.qty_shipped = 0;
            order.qty_rejected = req.quantity;
            order.error_code = ERR_INVALID_QUANTITY;

            build_response(&order, &resp);
            send_response(req.resp_fifo, &resp);
            continue;
        }

        if (produce_in_queue(args->pending_orders_q, &order) == -1) {
            break;
        }
    }

    return NULL;
}

static void *packer_thread_func(void *arg)
{
    PackerArgs *args = arg;
    unsigned int seed = (unsigned int)time(NULL) ^ (unsigned int)(uintptr_t)pthread_self();

    while (!shutdown_flag) {
        Order order;
        OrderResponse resp;

        if (consume_from_queue(args->packaging_q, &order) == -1) {
            break;
        }

        sleep_random_1_to_3(&seed);

        order.status = ORDER_SHIPPED;

        build_response(&order, &resp);
        send_response(order.request.resp_fifo, &resp);

        log_order(args->log_fd, &order);
    }

    return NULL;
}
static int read_restock_request(int restock_fd, RestockRequest *req)
{
    ssize_t n;

    pthread_mutex_lock(&restock_read_mutex);
    n = read_full(restock_fd, req, sizeof(*req));
    pthread_mutex_unlock(&restock_read_mutex);

    if (n == (ssize_t)sizeof(*req)) return 1;
    if (n == 0) return 0;
    return -1;
}

static void *restock_thread_func(void *arg)
{
    RestockArgs *args = arg;

    while (!shutdown_flag) {
        RestockRequest req;
        int rc;

        rc = read_restock_request(args->restock_fd, &req);

        if (rc == 0) {
            continue;
        }

        if (rc < 0) {
            if (shutdown_flag) break;
            fprintf(stderr, "[warehouse] failed to read RestockRequest\n");
            continue;
        }

        if (req.item_id == -1) {
            break;
        }

        if (inventory_increment(&inv, req.item_id, req.quantity) == -1) {
            fprintf(stderr,
                    "[warehouse] restock ignored: item_id=%d quantity=%d\n",
                    req.item_id,
                    req.quantity);
        }
    }

    return NULL;
}