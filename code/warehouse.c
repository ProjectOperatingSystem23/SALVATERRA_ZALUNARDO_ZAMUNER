/* ============================================================================
 * warehouse.c  --  Processo "Warehouse" del Fulfillment Center (Project 2026-3)
 *
 * Uso:
 *   ./warehouse <num_receivers> <num_pickers> <num_packers>
 *               <queue_capacity> <inventory.csv>
 *
 * COSA FA (spec 2.1 / 2.2):
 *   E' il cuore del sistema. Carica l'inventario, apre le FIFO di IPC e avvia
 *   quattro tipi di thread che cooperano in pipeline su due bounded buffer:
 *
 *       order.sh --FIFO--> [Receiver]* --pending--> [Picker]* --packaging--> [Packer]* --> orders.log
 *       supplier --FIFO--> [Restock] -> inventario
 *
 *   - Receiver (producer): legge OrderRequest dalla ORDERS_FIFO, valida, accoda
 *     nella "pending orders queue".
 *   - Picker (consumer+producer): estrae da pending, dorme 1-3s (lavoro fisico),
 *     decrementa lo stock in modo atomico, accoda in "packaging queue".
 *   - Packer (consumer): estrae da packaging, dorme 1-3s, spedisce, scrive su
 *     orders.log e risponde al client sulla sua FIFO privata.
 *   - Restock (consumer): legge RestockMsg dalla RESTOCK_FIFO (supplier o
 *     manage.sh) e incrementa lo stock.
 *
 * ----------------------------------------------------------------------------
 * SCELTE DI PROGETTO (per la relazione), tutte basate sulle slide del corso:
 *
 *  [I/O CON FILE DESCRIPTOR - Lab05]  Tutto l'I/O (inventario, log, FIFO,
 *      risposte) usa le system call open/read/write/close, NON gli stream
 *      <stdio.h>. I file descriptor sono il livello giusto per l'IPC su FIFO
 *      (Lab06) e per avere controllo su atomicita' ed errori. printf/fprintf
 *      restano usati SOLO per i messaggi a video (stdout/stderr).
 *
 *  [SINCRONIZZAZIONE - Lab04]  Le due code sono bounded buffer protetti da
 *      mutex + 2 condition variable (not_full / not_empty), col pattern
 *      "while(condizione) pthread_cond_wait(...)" mostrato a lezione. Niente
 *      busy-waiting/spinlock (spec 2.2.5): i thread bloccati NON consumano CPU
 *      (pthread_cond_wait rilascia il mutex e mette il thread a dormire nel
 *      kernel finche' non viene segnalato).
 *      L'inventario e' protetto da un singolo mutex (spec 2.2.3): le sezioni
 *      critiche (trova item + incrementa/decrementa) sono brevissime, quindi
 *      la contesa e' trascurabile e il codice resta semplice da verificare.
 *
 *  [SEGNALI - Lab03 / Lab09]  I segnali (SIGTERM/SIGINT/SIGUSR1) vengono
 *      BLOCCATI nel main prima di creare i thread (sigprocmask): i worker
 *      ereditano la maschera e NON vengono interrotti; i segnali arrivano solo
 *      al thread main. Gli handler fanno solo "set di un flag" volatile
 *      sig_atomic_t (async-signal-safe). Il main aspetta i segnali con
 *      sigsuspend (Lab09: sblocca+dormi in modo atomico, niente race).
 *
 *  [SHUTDOWN DEL THREAD RESTOCK - sentinella su FIFO, Lab06]  I supplier
 *      sono processi longevi: allo shutdown potrebbero tenere ancora aperto il
 *      lato scrittura della RESTOCK_FIFO, e una read bloccante non vedrebbe
 *      mai EOF (la pthread_join si appenderebbe). Soluzione con i soli
 *      strumenti del corso: il main, prima di chiudere la sua write-end dummy,
 *      scrive sulla FIFO un messaggio SENTINELLA (supplier_id=RESTOCK_STOP_ID)
 *      che il thread riconosce come ordine di uscita immediata.
 *
 *  [NIENTE VARIABILI GLOBALI (tranne i 2 flag dei segnali)]  Inventario, code,
 *      file descriptor e mutex sono creati nel main e passati ai thread PER
 *      RIFERIMENTO dentro struct-argomento (pattern thread_arg_t del Lab04).
 *      Ogni thread riceve solo i riferimenti che gli servono. I 2 flag dei
 *      segnali devono pero' essere globali, perche' gli handler ricevono solo
 *      "int sig".
 *
 * Riferimenti: Lab03 (segnali), Lab04 (thread/mutex/condvar), Lab05 (fd/IO),
 *              Lab06 (FIFO), Lab09 (sigsuspend). man 7 pipe.
 * ============================================================================ */

#define _POSIX_C_SOURCE 200809L  /* abilita sigaction, sigsuspend, mkfifo,
                                  * fcntl, ... (POSIX) */

#include <stdio.h>      /* printf/fprintf/snprintf, perror     (Lab05 errori) */
#include <stdlib.h>     /* malloc, free, atoi, exit                           */
#include <string.h>     /* memset, memcpy, strerror                           */
#include <unistd.h>     /* read, write, close, sleep, getpid   (Lab05)        */
#include <fcntl.h>      /* open, O_RDONLY/O_WRONLY/O_CREAT..., fcntl (Lab05)  */
#include <sys/stat.h>   /* mkfifo                              (Lab06)        */
#include <sys/types.h>
#include <pthread.h>    /* thread, mutex, condition variable   (Lab04)        */
#include <signal.h>     /* sigaction, sigprocmask, sigsuspend  (Lab03/09)     */
#include <errno.h>      /* errno                               (Lab05)        */
#include <time.h>       /* time                                               */
#include "common.h"     /* interfaccia binaria condivisa (struct + err code)  */

/* ═══════════════════════════════════════════════════════════════════════════
 * Costanti interne
 * ═══════════════════════════════════════════════════════════════════════════ */
#define MAX_INV_SIZE  1024   /* limite superiore sul numero di item nel CSV    */
#define LINE_BUF       512   /* buffer per una riga di log / status            */
#define RESTOCK_STOP_ID -1   /* supplier_id sentinella: inviato SOLO dal main
                             /* sulla propria write-end dummy per far uscire il
                              * thread restock allo shutdown. Un RestockMsg
                              * esterno con id <= 0 e' comunque scartato come
                              * non valido, quindi non e' falsificabile in modo
                              * utile dall'esterno. */
/* ═══════════════════════════════════════════════════════════════════════════
 * Strutture dati INTERNE (non escono dal processo -> non stanno in common.h)
 * ═══════════════════════════════════════════════════════════════════════════ */

/* Stato di un ordine lungo la pipeline (spec 2.2.2) */
typedef enum {
    ORDER_RECEIVED,
    ORDER_PICKING,
    ORDER_PACKING,
    ORDER_SHIPPED,
    ORDER_REJECTED
} OrderStatus;

/* Un item dell'inventario (spec 2.2.1) */
typedef struct {
    int  item_id;
    char description[MAX_DESC];
    char category[MAX_CATEGORY];
    int  stock;
} Item;

/* L'inventario: array di item + mutex che ne protegge lo stock (spec 2.2.3).
 * Un solo mutex per tutto l'array: le sezioni critiche (trova+decrementa) sono
 * cortissime, quindi la contesa e' trascurabile e il codice resta semplice. */
typedef struct {
    Item             items[MAX_INV_SIZE]; /* array statico: no malloc, no free */
    int              count;
    pthread_mutex_t  mutex;
} Inventory;

/* Un ordine "vivo" dentro il warehouse: incapsula la richiesta del client
 * (OrderRequest, da common.h) e aggiunge i campi di tracciamento interno. */
typedef struct {
    OrderRequest request;   /* client_id, resp_fifo, item_id, quantity         */
    int          order_id;  /* id progressivo interno                          */
    int          qty_shipped;
    int          qty_rejected;
    int          error_code;/* ERR_OK / ERR_PARTIAL / ERR_OUT_OF_STOCK / ...   */
    OrderStatus  status;
} Order;

/* Bounded buffer circolare protetto da mutex + 2 condition variable (Lab04).
 * not_full  : i producer (receiver/picker) ci aspettano quando la coda e' piena.
 * not_empty : i consumer (picker/packer)  ci aspettano quando la coda e' vuota.
 * shutdown  : alzato in chiusura per sbloccare TUTTI i thread in attesa.       */
typedef struct {
    Order           *buffer;
    int              head, tail, count, capacity;
    pthread_mutex_t  mutex;
    pthread_cond_t   not_full;
    pthread_cond_t   not_empty;
    int              shutdown;
} BoundedQueue;

/* ---- Struct-argomento passate ai thread (pattern Lab04) -------------------- *
 * Vivono nello stack del main e restano valide fino alla join: passare un
 * puntatore ad esse e' sicuro. Ogni ruolo riceve SOLO i riferimenti che usa.   */
typedef struct {
    int              orders_fd;          /* lato lettura della ORDERS_FIFO      */
    pthread_mutex_t *orders_read_mutex;  /* serializza la read tra piu' receiver*/
    Inventory       *inv;
    BoundedQueue    *pending;
    int             *next_order_id;      /* contatore condiviso degli order_id  */
    pthread_mutex_t *oid_mutex;
    int              log_fd;
    pthread_mutex_t *log_mutex;
} ReceiverArgs;

typedef struct {
    Inventory       *inv;
    BoundedQueue    *pending;
    BoundedQueue    *packaging;
    int              log_fd;
    pthread_mutex_t *log_mutex;
} PickerArgs;

typedef struct {
    BoundedQueue    *packaging;
    int              log_fd;
    pthread_mutex_t *log_mutex;
} PackerArgs;

typedef struct {
    int        restock_fd;               /* lato lettura della RESTOCK_FIFO     */
    Inventory *inv;
} RestockArgs;

/* ═══════════════════════════════════════════════════════════════════════════
 * UNICHE variabili globali: i flag dei segnali.
 *
 * volatile sig_atomic_t garantisce che lettura (nel main) e scrittura
 * (nell'handler) siano atomiche rispetto alla consegna del segnale e non
 * vengano riordinate/ottimizzate via dal compilatore (Lab03).
 * ═══════════════════════════════════════════════════════════════════════════ */
static volatile sig_atomic_t g_shutdown    = 0;   /* SIGTERM / SIGINT          */
static volatile sig_atomic_t g_dump_status = 0;   /* SIGUSR1                   */

/* ═══════════════════════════════════════════════════════════════════════════
 * I/O di basso livello su file descriptor (Lab05)
 * ═══════════════════════════════════════════════════════════════════════════ */

/* read "completa": ripete la read finche' non ha letto len byte.
 * Ritorna i byte letti (== len caso normale, < len se arriva EOF prima,
 * 0 se EOF immediato) oppure -1 su errore. Gestisce EINTR (Lab05).
 * NB sul buf "void *": e' il puntatore generico del C, cosi' la funzione
 * accetta qualunque tipo senza cast da parte del chiamante; il cast a char*
 * serve solo internamente per l'aritmetica byte per byte. */
static ssize_t read_all(int fd, void *buf, size_t len)
{
    size_t done = 0;
    while (done < len) {
        ssize_t n = read(fd, (char *)buf + done, len - done);
        if (n < 0) {
            if (errno == EINTR) continue;   /* interrotta da segnale: riprova  */
            return -1;
        }
        if (n == 0) break;                  /* EOF                             */
        done += (size_t)n;
    }
    return (ssize_t)done;
}

/* write "completa": ripete la write finche' non ha scritto len byte (Lab05). */
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

/* ═══════════════════════════════════════════════════════════════════════════
 * Inventario: caricamento da CSV via file descriptor + accesso sincronizzato
 * ═══════════════════════════════════════════════════════════════════════════ */

/* Estrae UN campo CSV dal cursore *pp. Gestisce sia campi "tra virgolette"
 * (es. "Wireless Mouse") sia campi semplici. Avanza *pp oltre il campo e la
 * virgola successiva. Scrive al massimo dst_size-1 byte e NUL-termina. */
static void csv_field(char **pp, char *dst, int dst_size)
{
    char *p = *pp;
    int   i = 0;

    if (*p == '"') {                 /* campo quotato                           */
        p++;
        while (*p && *p != '"') {
            if (i < dst_size - 1) dst[i++] = *p;
            p++;
        }
        if (*p == '"') p++;          /* salta la virgoletta di chiusura         */
    } else {                         /* campo semplice                          */
        while (*p && *p != ',' && *p != '\n' && *p != '\r') {
            if (i < dst_size - 1) dst[i++] = *p;
            p++;
        }
    }
    dst[i] = '\0';
    if (*p == ',') p++;              /* salta il separatore                     */
    *pp = p;
}

/* Legge una riga dall'fd nel buffer. Ritorna i byte letti, 0=EOF, -1=errore.
 * Lettura byte per byte: con i soli fd (niente stdio) non c'e' una "readline"
 * pronta; leggere 1 byte alla volta e' la soluzione piu' semplice e corretta,
 * e il caricamento del CSV avviene una sola volta all'avvio, quindi
 * l'inefficienza e' irrilevante. */
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

static int inventory_load(Inventory *inv, const char *path)
{
    int fd = open(path, O_RDONLY);
    if (fd < 0) {
        fprintf(stderr, "[WAREHOUSE] open inventario '%s': %s\n",
                path, strerror(errno));
        return ERR_IO;
    }

    char line[512];
    /* salta l'header */
    if (fd_read_line(fd, line, sizeof(line)) <= 0) {
        fprintf(stderr, "[WAREHOUSE] inventario '%s' vuoto\n", path);
        close(fd);
        return ERR_IO;
    }

    inv->count = 0;
    while (fd_read_line(fd, line, sizeof(line)) > 0) {
        if (line[0] == '\r' || line[0] == '\n' || line[0] == '\0') continue;
        if (inv->count >= MAX_INV_SIZE) {
            fprintf(stderr, "[WAREHOUSE] inventario troncato a %d item\n",
                    MAX_INV_SIZE);
            break;
        }
        Item *it = &inv->items[inv->count];
        char *p  = line;
        char  tmp[32];

        csv_field(&p, tmp, sizeof(tmp));
        it->item_id = atoi(tmp);
        if (it->item_id <= 0) continue;          /* riga malformata */

        csv_field(&p, it->description, sizeof(it->description));
        csv_field(&p, it->category,    sizeof(it->category));

        csv_field(&p, tmp, sizeof(tmp));
        it->stock = atoi(tmp);
        if (it->stock < 0) it->stock = 0;        /* difensivo: mai stock < 0 */

        inv->count++;
    }

    close(fd);

    if (inv->count == 0) {
        fprintf(stderr, "[WAREHOUSE] nessun item valido in '%s'\n", path);
        return ERR_IO;
    }
    return ERR_OK;
}

/* Trova l'item per id. Il chiamante DEVE gia' tenere inv->mutex. */
static Item *inventory_find_locked(Inventory *inv, int item_id)
{
    for (int i = 0; i < inv->count; i++)
        if (inv->items[i].item_id == item_id)
            return &inv->items[i];
    return NULL;
}

/* Decremento ATOMICO dello stock (chiamato dal picker mentre tiene inv->mutex).
 * Ritorna le unita' effettivamente spedite (0..qty) e imposta *err.
 * E' QUI che si risolve la race "ultime unita' ordinate insieme" (spec 2.2.10):
 * essendo tutto dentro inv->mutex, due picker non possono entrambi spedire
 * la stessa unita'. Il check "stock <= 0" copre il caso in cui un altro picker
 * abbia esaurito l'item tra la validazione del receiver e questo momento. */
static int inventory_decrement_locked(Inventory *inv, int item_id, int qty,
                                      int *err)
{
    Item *it = inventory_find_locked(inv, item_id);
    if (!it)            { *err = ERR_ITEM_NOT_FOUND; return 0; }
    if (it->stock <= 0) { *err = ERR_OUT_OF_STOCK;   return 0; }

    int ship = (it->stock >= qty) ? qty : it->stock;  /* riempimento parziale  */
    it->stock -= ship;
    *err = (ship < qty) ? ERR_PARTIAL : ERR_OK;
    return ship;
}

/* Incremento dello stock (chiamato dal thread restock). Prende lui il lock.
 * NB: pthread_mutex_lock NON e' uno spinlock: se il mutex e' occupato il
 * thread viene messo a dormire dal kernel (futex), zero CPU consumata.
 * Ritorna il nuovo stock, o -1 se l'item non esiste. */
static int inventory_increment(Inventory *inv, int item_id, int qty)
{
    int newstock = -1;
    pthread_mutex_lock(&inv->mutex);
    Item *it = inventory_find_locked(inv, item_id);
    if (it) { it->stock += qty; newstock = it->stock; }
    pthread_mutex_unlock(&inv->mutex);
    return newstock;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Bounded buffer (Lab04: mutex + condition variable)
 * ═══════════════════════════════════════════════════════════════════════════ */
static int bq_init(BoundedQueue *q, int capacity)
{
    q->buffer = malloc((size_t)capacity * sizeof(Order));
    if (!q->buffer) return ERR_IO;
    q->head = q->tail = q->count = 0;
    q->capacity = capacity;
    q->shutdown = 0;

    if (pthread_mutex_init(&q->mutex, NULL) != 0) {
        free(q->buffer);
        return ERR_IO;
    }
    if (pthread_cond_init(&q->not_empty, NULL) != 0) {
        pthread_mutex_destroy(&q->mutex);
        free(q->buffer);
        return ERR_IO;
    }
    if (pthread_cond_init(&q->not_full, NULL) != 0) {
        pthread_cond_destroy(&q->not_empty);
        pthread_mutex_destroy(&q->mutex);
        free(q->buffer);
        return ERR_IO;
    }
    return ERR_OK;
}

static void bq_destroy(BoundedQueue *q)
{
    pthread_mutex_destroy(&q->mutex);
    pthread_cond_destroy (&q->not_full);
    pthread_cond_destroy (&q->not_empty);
    free(q->buffer);
}

/* Inserisce un ordine. BLOCCA (senza consumare CPU) se la coda e' piena, finche'
 * un consumer libera spazio (spec 2.2.5). Ritorna 0, oppure -1 se la coda e' in
 * shutdown ED e' piena (rinuncia: serve a non restare bloccati per sempre).
 * NB: con la sequenza di shutdown del main (vedi sotto) i producer vengono
 * joinati PRIMA di alzare lo shutdown sulla loro coda di uscita, quindi il
 * ramo -1 e' solo difensivo: nessun ordine in volo viene davvero scartato. */
static int bq_push(BoundedQueue *q, const Order *o)
{
    pthread_mutex_lock(&q->mutex);
    while (q->count == q->capacity && !q->shutdown)        /* pattern Lab04     */
        pthread_cond_wait(&q->not_full, &q->mutex);
    if (q->count == q->capacity && q->shutdown) {/* pieno e in chiusura */
        pthread_cond_signal(&q->not_full);   /* risveglio a catena (vedi bq_pop) */
        pthread_mutex_unlock(&q->mutex);
        return -1;
    }
    q->buffer[q->tail] = *o;
    q->tail = (q->tail + 1) % q->capacity;
    q->count++;
    pthread_cond_signal(&q->not_empty);                    /* sveglia un consumer */
    pthread_mutex_unlock(&q->mutex);
    return 0;
}

/* Estrae un ordine. BLOCCA (senza CPU) se la coda e' vuota. Ritorna 0, oppure
 * -1 se la coda e' vuota E in shutdown: e' il segnale per il thread di uscire.
 * NB: se in shutdown ma ci sono ancora elementi, ogni chiamata ne estrae uno:
 * i consumer continuano a ciclare finche' la coda non e' DAVVERO vuota, quindi
 * gli ordini "in volo" vengono completati prima della chiusura (spec 2.2.10). */
static int bq_pop(BoundedQueue *q, Order *out)
{
    pthread_mutex_lock(&q->mutex);
    while (q->count == 0 && !q->shutdown)
        pthread_cond_wait(&q->not_empty, &q->mutex);
    if (q->count == 0) {                                   /* vuota + shutdown  */
        /* RISVEGLIO A CATENA: prima di uscire risveglio il prossimo consumer
         * in attesa, che a sua volta vedra' vuota+shutdown, risegnalera' e
         * uscira'. Cosi' basta UNA pthread_cond_signal in bq_shutdown per
         * svegliare tutti, senza bisogno di broadcast (Lab04 usa signal). */
        pthread_cond_signal(&q->not_empty);
        pthread_mutex_unlock(&q->mutex);
        return -1;
    }
    *out = q->buffer[q->head];
    q->head = (q->head + 1) % q->capacity;
    q->count--;
    pthread_cond_signal(&q->not_full);                     /* sveglia un producer */
    pthread_mutex_unlock(&q->mutex);
    return 0;
}

/* Alza shutdown e sveglia TUTTI i thread bloccati sulle due condvar. */
static void bq_shutdown(BoundedQueue *q)
{
    pthread_mutex_lock(&q->mutex);
    q->shutdown = 1;
    pthread_cond_broadcast(&q->not_full);
    pthread_cond_broadcast(&q->not_empty);
    pthread_mutex_unlock(&q->mutex);
}

/* Numero di elementi correnti (lettura sincronizzata, per lo status dump). */
static int bq_size(BoundedQueue *q)
{
    pthread_mutex_lock(&q->mutex);
    int c = q->count;
    pthread_mutex_unlock(&q->mutex);
    return c;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Logging su orders.log via file descriptor (Lab05)
 *
 * Formato (pipe-separated, facile da analizzare con grep/awk/wc in manage.sh):
 *   timestamp|order_id|client_id|item_id|qty_req|qty_shipped|qty_rejected|STATUS
 * STATUS in { SHIPPED, PARTIAL, REJECTED }.
 * ═══════════════════════════════════════════════════════════════════════════ */
static void log_order(int log_fd, pthread_mutex_t *log_mutex, const Order *o)
{
    char tbuf[32];
    time_t now = time(NULL);
    snprintf(tbuf, sizeof(tbuf), "%ld", (long)now);  /* unix timestamp grezzo */

    const char *st = "SHIPPED";
    if      (o->status == ORDER_REJECTED)  st = "REJECTED";
    else if (o->error_code == ERR_PARTIAL) st = "PARTIAL";

    char line[LINE_BUF];
    int n = snprintf(line, sizeof(line), "%s|%d|%s|%d|%d|%d|%d|%s\n",
                     tbuf, o->order_id, o->request.client_id, o->request.item_id,
                     o->request.quantity, o->qty_shipped, o->qty_rejected, st);
    if (n <= 0) return;

    /* Il file e' aperto in O_APPEND, quindi una singola write e' gia' atomica;
     * teniamo comunque un mutex (Lab04) per non dipendere dal comportamento
     * di filesystem particolari e per documentare l'intento. */
    pthread_mutex_lock(log_mutex);
    write_all(log_fd, line, (size_t)n);
    pthread_mutex_unlock(log_mutex);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Risposta al client sulla sua FIFO privata (Lab06: 2 FIFO = bidirezionale)
 *
 * Il PROTOCOLLO del client (order_client.c) garantisce che il lato lettura
 * della resp_fifo venga aperto PRIMA di inviare la richiesta: quindi, se il
 * client e' vivo, la open qui sotto riesce subito. La apriamo con O_NONBLOCK
 * per NON bloccarci se il client e' morto nel frattempo (in quel caso open
 * fallisce con ENXIO "no reader" o ENOENT "fifo rimossa"): senza O_NONBLOCK
 * un packer resterebbe appeso per sempre su un client defunto.
 * La risposta resta in formato BINARIO (OrderResponse): e' order_client a
 * tradurla in output human-readable per l'utente.
 * ═══════════════════════════════════════════════════════════════════════════ */
static void send_response(const char *resp_fifo, const OrderResponse *resp)
{
    if (resp_fifo == NULL || resp_fifo[0] == '\0') return;
    int fd = open(resp_fifo, O_WRONLY | O_NONBLOCK);/*TODO: DA METTERE NEL REPORT PERCHÉ APRIAMO IN NON BLOCCANTE (PERCHÉ SE MUORE IL CLIENT NON STIAMO LI BLOCCATI AD ASPETTARE)*/
    if (fd < 0) {
        fprintf(stderr, "[WAREHOUSE] risposta non recapitabile su '%s': %s "
                        "(client terminato?)\n", resp_fifo, strerror(errno));
        return;
    }
    if (write_all(fd, resp, sizeof(*resp)) < 0)
        fprintf(stderr, "[WAREHOUSE] write risposta su '%s': %s\n",
                resp_fifo, strerror(errno));
    close(fd);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Utility varie
 * ═══════════════════════════════════════════════════════════════════════════ */

/* Dorme un tempo casuale 1-3 s per simulare il lavoro fisico (spec 2.2.7).
 * srand() viene chiamato una volta sola nel main. */
static void rand_sleep_1_3(void)
{
    sleep(1 + rand() % 3);    /* uniforme in {1,2,3} secondi */
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Gestione segnali (Lab03)  --  gli handler fanno SOLO set di flag
 * ═══════════════════════════════════════════════════════════════════════════ */
static void handle_shutdown(int sig) { (void)sig; g_shutdown    = 1; }
static void handle_dump    (int sig) { (void)sig; g_dump_status = 1; }

/* Helper per registrare un handler con sigaction (pattern Lab03/Lab04). */
static void setup_handler(int sig, void (*fn)(int))
{
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = fn;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(sig, &sa, NULL);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Dump dello stato su STATUS_FILE (per manage.sh status)
 *
 * Chiamato dal MAIN (non dall'handler!) quando g_dump_status e' alzato: cosi'
 * possiamo usare lock e write() in tutta sicurezza (un signal handler non
 * potrebbe, perche' quelle funzioni non sono async-signal-safe). Lab05 fd.
 * ═══════════════════════════════════════════════════════════════════════════ */
static void do_status_dump(Inventory *inv, BoundedQueue *pending,
                           BoundedQueue *packaging, int nr, int np, int npk)
{
    int fd = open(STATUS_FILE, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        fprintf(stderr, "[WAREHOUSE] open status '%s': %s\n",
                STATUS_FILE, strerror(errno));
        return;
    }

    char buf[LINE_BUF];
    int  n;
    n = snprintf(buf, sizeof(buf), "PID=%d\n", (int)getpid());
    if (n > 0) write_all(fd, buf, (size_t)n);
    n = snprintf(buf, sizeof(buf), "RECEIVERS=%d\nPICKERS=%d\nPACKERS=%d\n",
                 nr, np, npk);
    if (n > 0) write_all(fd, buf, (size_t)n);
    n = snprintf(buf, sizeof(buf), "PENDING_QUEUE=%d/%d\n",
                 bq_size(pending), pending->capacity);
    if (n > 0) write_all(fd, buf, (size_t)n);
    n = snprintf(buf, sizeof(buf), "PACKAGING_QUEUE=%d/%d\n",
                 bq_size(packaging), packaging->capacity);
    if (n > 0) write_all(fd, buf, (size_t)n);

    /* Copia dell'inventario sotto lock, poi scrittura FUORI dal lock: cosi'
     * teniamo inv->mutex solo per la memcpy e non durante le write(). */
    pthread_mutex_lock(&inv->mutex);
    int   count = inv->count;
    Item *snap  = malloc((size_t)count * sizeof(Item));
    if (snap) memcpy(snap, inv->items, (size_t)count * sizeof(Item));
    pthread_mutex_unlock(&inv->mutex);

    n = snprintf(buf, sizeof(buf), "INVENTORY_COUNT=%d\n", count);
    if (n > 0) write_all(fd, buf, (size_t)n);
    if (snap) {
        for (int i = 0; i < count; i++) {
            n = snprintf(buf, sizeof(buf), "ITEM|%d|%s|%s|%d\n",
                         snap[i].item_id, snap[i].description,
                         snap[i].category, snap[i].stock);
            if (n > 0) write_all(fd, buf, (size_t)n);
        }
        free(snap);
    }
    close(fd);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * THREAD: Order Receiver (producer)  --  spec 2.2.4
 *
 * Piu' receiver leggono dalla STESSA FIFO: un mutex sulla read garantisce che
 * ciascuno prenda esattamente UN OrderRequest completo (Lab04 mutex + Lab06).
 * ═══════════════════════════════════════════════════════════════════════════ */
static void log_rejection(int log_fd, pthread_mutex_t *lm,
                          const OrderRequest *req, int oid, int err)
{
    Order o;
    memset(&o, 0, sizeof(o));
    o.request      = *req;
    o.order_id     = oid;
    o.status       = ORDER_REJECTED;
    o.error_code   = err;
    o.qty_rejected = req->quantity;
    log_order(log_fd, lm, &o);
}

static void *receiver_thread(void *arg)
{
    ReceiverArgs *a = (ReceiverArgs *)arg;

    while (!g_shutdown) {
        OrderRequest req;
        /*TODO: SCRIVERE REPORT CHE LE STRUCT SONO MINORI DI PIPEBUF QUINDI LE WRITE SONO ATOMICHE*/
        /* read mutuamente esclusiva: un solo receiver alla volta legge la FIFO.
         * Le write dei client sono atomiche (< PIPE_BUF), quindi nella FIFO ci
         * sono sempre messaggi interi: serializzare le read garantisce che ogni
         * receiver estragga esattamente un messaggio. */
        pthread_mutex_lock(a->orders_read_mutex);
        ssize_t n = read_all(a->orders_fd, &req, sizeof(req));
        pthread_mutex_unlock(a->orders_read_mutex);

        if (n == 0) break;                       /* EOF: write-end dummy chiusa */
        if (n != (ssize_t)sizeof(req)) {         /* errore / messaggio troncato */
            if (n < 0) perror("[RECEIVER] read");/* EINTR gia' gestito in read_all */
            if (g_shutdown) break;
            continue;
        }
        /*siccome abbiamo letto da una fifo info da un processo esterno, per principio mettiamo il terminatore*/
        req.client_id[MAX_CLIENT_ID - 1] = '\0';
        req.resp_fifo[MAX_RESP_FIFO - 1] = '\0';
        /* order_id progressivo: contatore condiviso protetto da mutex */
        pthread_mutex_lock(a->oid_mutex);
        int oid = (*a->next_order_id)++;
        pthread_mutex_unlock(a->oid_mutex);

        OrderResponse resp;
        memset(&resp, 0, sizeof(resp));
        resp.item_id       = req.item_id;
        resp.qty_requested = req.quantity;

        /* --- Validazioni (spec 2.2.4 / 2.2.10) --- */
        if (req.quantity <= 0) {                                /* qty 0 o < 0 */
            resp.status = ERR_INVALID_QTY; resp.qty_rejected = req.quantity;
            send_response(req.resp_fifo, &resp);
            log_rejection(a->log_fd, a->log_mutex, &req, oid, ERR_INVALID_QTY);
            continue;
        }
        if (req.item_id <= 0) {                  /* id non valido per contratto */
            resp.status = ERR_ITEM_NOT_FOUND; resp.qty_rejected = req.quantity;
            send_response(req.resp_fifo, &resp);
            log_rejection(a->log_fd, a->log_mutex, &req, oid, ERR_ITEM_NOT_FOUND);
            continue;
        }

        /* Peek esistenza/stock sotto lock: e' la validazione "leggera" che la
         * spec assegna ai receiver ("check that the item exists and is in
         * stock"). Il controllo DEFINITIVO sullo stock resta nel picker
         * (inventory_decrement_locked): tra questo peek e il pick lo stock
         * puo' cambiare, ed e' li' che la race viene davvero risolta. */
        pthread_mutex_lock(&a->inv->mutex);
        Item *it       = inventory_find_locked(a->inv, req.item_id);
        int   exists   = (it != NULL);
        int   in_stock = exists && it->stock > 0;
        pthread_mutex_unlock(&a->inv->mutex);

        if (!exists) {
            resp.status = ERR_ITEM_NOT_FOUND; resp.qty_rejected = req.quantity;
            send_response(req.resp_fifo, &resp);
            log_rejection(a->log_fd, a->log_mutex, &req, oid, ERR_ITEM_NOT_FOUND);
            continue;
        }
        if (!in_stock) {                /* out of stock: rifiuto immediato (spec 2.2.2) */
            resp.status = ERR_OUT_OF_STOCK; resp.qty_rejected = req.quantity;
            send_response(req.resp_fifo, &resp);
            log_rejection(a->log_fd, a->log_mutex, &req, oid, ERR_OUT_OF_STOCK);
            continue;
        }

        /* --- Accoda nella pending queue (puo' bloccare se piena: spec 2.2.5) --- */
        Order o;
        memset(&o, 0, sizeof(o));
        o.request  = req;
        o.order_id = oid;
        o.status   = ORDER_RECEIVED;
        if (bq_push(a->pending, &o) != 0) break;   /* solo se coda in shutdown  */
    }
    return NULL;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * THREAD: Picker Robot (consumer di pending, producer di packaging) - spec 2.2.4
 * ═══════════════════════════════════════════════════════════════════════════ */
static void *picker_thread(void *arg)
{
    PickerArgs *a = (PickerArgs *)arg;

    for (;;) {
        Order o;
        if (bq_pop(a->pending, &o) != 0) break;       /* vuota + shutdown -> esci */

        o.status = ORDER_PICKING;

        rand_sleep_1_3();                             /* tempo di prelievo (spec) */

        /* Decremento atomico dello stock sotto inv->mutex */
        int err;
        pthread_mutex_lock(&a->inv->mutex);
        int shipped = inventory_decrement_locked(a->inv, o.request.item_id,
                                                 o.request.quantity, &err);
        pthread_mutex_unlock(&a->inv->mutex);

        o.qty_shipped  = shipped;
        o.qty_rejected = o.request.quantity - shipped;
        o.error_code   = err;

        if (shipped == 0) {        /* item esaurito tra validazione e pick */
            o.status = ORDER_REJECTED;
            OrderResponse resp = { err, o.request.item_id, o.request.quantity,
                                   0, o.request.quantity };
            send_response(o.request.resp_fifo, &resp);
            log_order(a->log_fd, a->log_mutex, &o);
            continue;
        }

        /* Inoltra al packaging (anche se parziale: la parte disponibile va spedita) */
        o.status = ORDER_PACKING;
        if (bq_push(a->packaging, &o) != 0) break;    /* solo se coda in shutdown */
    }
    return NULL;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * THREAD: Packer (consumer di packaging) - spec 2.2.4
 * ═══════════════════════════════════════════════════════════════════════════ */
static void *packer_thread(void *arg)
{
    PackerArgs *a = (PackerArgs *)arg;

    for (;;) {
        Order o;
        if (bq_pop(a->packaging, &o) != 0) break;

        rand_sleep_1_3();                             /* tempo di imballaggio */

        o.status = ORDER_SHIPPED;
        int status = (o.error_code == ERR_PARTIAL) ? ERR_PARTIAL : ERR_OK;
        OrderResponse resp = { status, o.request.item_id, o.request.quantity,
                               o.qty_shipped, o.qty_rejected };
        send_response(o.request.resp_fifo, &resp);
        log_order(a->log_fd, a->log_mutex, &o);
    }
    return NULL;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * THREAD: Restock listener (consumer della RESTOCK_FIFO) - spec 2.2.6
 *
 *
 * Gira CONCORRENTEMENTE all'elaborazione ordini: usa lo stesso inv->mutex dei
 * picker, quindi increment e decrement non si pestano i piedi (spec 2.2.7).
 *
 * A differenza dei receiver, qui i writer (supplier) sono processi LONGEVI:
 * allo shutdown potrebbero tenere ancora aperto il lato scrittura della FIFO,
 * e una read bloccante non vedrebbe mai EOF (il warehouse si appenderebbe in
 * pthread_join). Per questo il thread esce anche quando legge il messaggio
 * SENTINELLA (supplier_id == RESTOCK_STOP_ID) che il main scrive sulla
 * propria write-end dummy allo shutdown: solo read/write su FIFO (Lab06).
 * Un supplier crashato/killato (spec 2.2.10) non blocca nulla: semplicemente
 * smette di mandare messaggi, e la read resta in attesa degli altri writer.
 * ═══════════════════════════════════════════════════════════════════════════ */
static void *restock_thread(void *arg)
{
    RestockArgs *a = (RestockArgs *)arg;

    for (;;) {
        RestockMsg msg;
        ssize_t n = read_all(a->restock_fd, &msg, sizeof(msg));

        if (n == 0) break;                          /* EOF: nessun writer       */
        if (n != (ssize_t)sizeof(msg)) {
            if (n < 0) perror("[RESTOCK] read");    /* EINTR gestito in read_all */
            if (g_shutdown) break;
            continue;
        }
        /*SIAMO SICURI CHE IL -1 NON SIA INVIATO DA UN SUPPLIER MALEVOLO PERCHÉ IL C HELPER FA CONTROLLO INPUT DEL BASH SCRIPT*/
        if (msg.supplier_id == RESTOCK_STOP_ID)     /* sentinella dal main:     */
            break;                                  /* shutdown, esci subito    */
        if (msg.item_id <= 0 || msg.quantity <= 0) {
            fprintf(stderr, "[RESTOCK] messaggio non valido (item=%d qty=%d)\n",
                    msg.item_id, msg.quantity);
            continue;
        }

        int newstock = inventory_increment(a->inv, msg.item_id, msg.quantity);
        if (newstock < 0)
            fprintf(stderr, "[RESTOCK] item_id=%d non trovato\n", msg.item_id);
    }
    return NULL;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Apertura di una FIFO in lettura + write-end "dummy" (Lab06)
 *TODO: BELLO, METTILO NEL REPORT
 * Trucco standard: teniamo SEMPRE aperta una write-end nostra, cosi' la read()
 * dei thread non vede mai EOF mentre il sistema e' attivo (altrimenti, appena
 * l'ultimo client chiude, read() tornerebbe 0). In chiusura, il main chiude la
 * write-end dummy: a quel punto read() ritorna 0 (EOF) e il thread esce pulito.
 *
 * Sequenza delle open:
 *   1. read-end con O_NONBLOCK: senza, open(O_RDONLY) bloccherebbe finche'
 *      non esiste un writer (Lab06) -- ma il writer siamo noi al passo 2.
 *   2. write-end SENZA O_NONBLOCK: non serve, il lato lettura e' gia' aperto
 *      (siamo noi), quindi la open non puo' bloccare.
 *   3. fcntl per togliere O_NONBLOCK dal read-end: d'ora in poi le read dei
 *      thread sono BLOCCANTI (i thread dormono, niente busy-wait, spec 2.2.5).
 * ═══════════════════════════════════════════════════════════════════════════ */
static int open_fifo_rw(const char *path, int *read_fd, int *dummy_write_fd)
{
    if (mkfifo(path, 0666) != 0 && errno != EEXIST) {   /* idempotente          */
        fprintf(stderr, "[WAREHOUSE] mkfifo '%s': %s\n", path, strerror(errno));
        return -1;
    }
    int rfd = open(path, O_RDONLY | O_NONBLOCK);
    if (rfd < 0) {
        fprintf(stderr, "[WAREHOUSE] open(read) '%s': %s\n", path, strerror(errno));
        return -1;
    }
    int wfd = open(path, O_WRONLY);                      /* dummy write-end     */
    if (wfd < 0) {
        fprintf(stderr, "[WAREHOUSE] open(write) '%s': %s\n", path, strerror(errno));
        close(rfd);
        return -1;
    }
    /*TODO: QUESTI COMANDI NON SONO NEI LAB, VA BENE USARLI MA ESSERE PRONTI ALLE FRANZILLATE*/
    int fl = fcntl(rfd, F_GETFL, 0);                    /* togli O_NONBLOCK:   */
    if (fl < 0 || fcntl(rfd, F_SETFL, fl & ~O_NONBLOCK) < 0) {
        fprintf(stderr, "[WAREHOUSE] fcntl '%s': %s\n", path, strerror(errno));
        close(rfd); close(wfd);
        return -1;
    }
    *read_fd = rfd;
    *dummy_write_fd = wfd;
    return 0;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * MAIN
 * ═══════════════════════════════════════════════════════════════════════════ */
int main(int argc, char *argv[])
{
    /* ---- parsing + validazione argomenti (spec 2.3) ---- */
    if (argc != 6) {
        fprintf(stderr, "Usage: %s <num_receivers> <num_pickers> <num_packers>"
                        " <queue_capacity> <inventory.csv>\n", argv[0]);
        return EXIT_FAILURE;
    }
    int num_receivers = atoi(argv[1]);
    int num_pickers   = atoi(argv[2]);
    int num_packers   = atoi(argv[3]);
    int queue_cap     = atoi(argv[4]);
    const char *inv_path = argv[5];

    if (num_receivers <= 0 || num_pickers <= 0 ||
        num_packers   <= 0 || queue_cap   <= 0) {
        fprintf(stderr, "[WAREHOUSE] tutti i parametri numerici devono essere > 0\n");
        return EXIT_FAILURE;
    }
    srand((unsigned)time(NULL) ^ (unsigned)getpid());

    /* ---- inventario (Lab05 fd + Lab04 mutex) ---- */
    Inventory inv;
    if (pthread_mutex_init(&inv.mutex, NULL) != 0) {
        fprintf(stderr, "[WAREHOUSE] init mutex inventario fallita\n");
        return EXIT_FAILURE;
    }
    if (inventory_load(&inv, inv_path) != ERR_OK)
        return EXIT_FAILURE;

    /* ---- log: open in append (Lab05) ---- */
    /*TODO: VEDERE CHE BOOTSTRAP ABBIA FATTO PULIZIA, CHE NON
     *CI SIANO ISTANZE PRECEDENTI DI QUESTO FILE*/
    int log_fd = open(LOG_FILE, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (log_fd < 0) {
        fprintf(stderr, "[WAREHOUSE] open log '%s': %s\n", LOG_FILE, strerror(errno));
        return EXIT_FAILURE;
    }
    pthread_mutex_t log_mutex;
    pthread_mutex_init(&log_mutex, NULL);

    /* ---- code bounded (Lab04) ---- */
    BoundedQueue pending, packaging;
    if (bq_init(&pending, queue_cap) != ERR_OK ||
        bq_init(&packaging, queue_cap) != ERR_OK) {
        fprintf(stderr, "[WAREHOUSE] init code fallita\n");
        return EXIT_FAILURE;
    }

    /* ---- FIFO (Lab06) ---- */
    int orders_fd, orders_dummy_fd, restock_fd, restock_dummy_fd;
    if (open_fifo_rw(ORDERS_FIFO,  &orders_fd,  &orders_dummy_fd)  != 0) return EXIT_FAILURE;
    if (open_fifo_rw(RESTOCK_FIFO, &restock_fd, &restock_dummy_fd) != 0) return EXIT_FAILURE;
    pthread_mutex_t orders_read_mutex;
    pthread_mutex_init(&orders_read_mutex, NULL);

    /* ---- contatore order_id condiviso ---- */
    int next_order_id = 1;
    pthread_mutex_t oid_mutex;
    pthread_mutex_init(&oid_mutex, NULL);

    /* ---- segnali (Lab03/09) ----
     * Blocchiamo i segnali ORA, mentre il processo e' ancora a thread singolo:
     * la maschera viene EREDITATA dai thread creati dopo, che quindi non li
     * riceveranno. (sigprocmask e' ben definito perche' siamo single-thread;
     * pthread_sigmask sarebbe l'equivalente a thread gia' avviati.) */
    /*TODO: STUDIARSI QUESTE COSE DA SAPERLE SPIEGARE*/
    sigset_t block_set, empty_set;
    sigemptyset(&block_set);
    sigaddset(&block_set, SIGTERM);
    sigaddset(&block_set, SIGINT);
    sigaddset(&block_set, SIGUSR1);
    sigprocmask(SIG_BLOCK, &block_set, NULL);
    sigemptyset(&empty_set);               /* maschera vuota per sigsuspend */

    setup_handler(SIGTERM, handle_shutdown);  /* handler: solo set di flag */
    setup_handler(SIGINT,  handle_shutdown);
    setup_handler(SIGUSR1, handle_dump);

    /* SIGPIPE ignorato: se un client chiude la sua resp_fifo, la write deve
     * fallire con EPIPE (gestito), NON terminare il warehouse. */
    /*TODO: VEDERE CHE SIA LAB APPROVED, e magari se ritenuto necessario scrivere nel report*/
    struct sigaction ign;
    memset(&ign, 0, sizeof(ign));
    ign.sa_handler = SIG_IGN; /*costante speciale ignora, il kernel scarta il segnale senza consegnarlo*/
    sigemptyset(&ign.sa_mask);
    sigaction(SIGPIPE, &ign, NULL);

    /* ---- struct-argomento (riferimenti, niente globali: Lab04) ---- */
    ReceiverArgs ra = { orders_fd, &orders_read_mutex, &inv, &pending,
                        &next_order_id, &oid_mutex, log_fd, &log_mutex };
    PickerArgs   pa = { &inv, &pending, &packaging, log_fd, &log_mutex };
    PackerArgs   ka = { &packaging, log_fd, &log_mutex };
    RestockArgs  sa = { restock_fd, &inv };

    /* ---- spawn dei thread pool (Lab04) ---- */
    pthread_t *recv_th = malloc((size_t)num_receivers * sizeof(pthread_t));
    pthread_t *pick_th = malloc((size_t)num_pickers   * sizeof(pthread_t));
    pthread_t *pack_th = malloc((size_t)num_packers   * sizeof(pthread_t));
    pthread_t  restock_th;
    if (!recv_th || !pick_th || !pack_th) {
        fprintf(stderr, "[WAREHOUSE] malloc dei thread fallita\n");
        return EXIT_FAILURE;
    }

    for (int i = 0; i < num_receivers; i++) pthread_create(&recv_th[i], NULL, receiver_thread, &ra);
    for (int i = 0; i < num_pickers;   i++) pthread_create(&pick_th[i], NULL, picker_thread,   &pa);
    for (int i = 0; i < num_packers;   i++) pthread_create(&pack_th[i], NULL, packer_thread,   &ka);
    pthread_create(&restock_th, NULL, restock_thread, &sa);

    /* ---- loop principale: aspetta i segnali (Lab09 sigsuspend, race-free) ----
     * sigsuspend sblocca atomicamente i segnali e dorme finche' ne arriva uno;
     * al ritorno la maschera (bloccante) e' ripristinata. Niente busy-wait. */
    while (!g_shutdown) {
        sigsuspend(&empty_set);
        if (g_dump_status) {
            do_status_dump(&inv, &pending, &packaging,
                           num_receivers, num_pickers, num_packers);
            g_dump_status = 0;
        }
    }

    /* ---- shutdown ORDINATO (spec 2.2.10: completa gli ordini in volo) ----
     * Sequenza pensata per NON perdere ordini gia' entrati nella pipeline:
     *   1. scrivo la SENTINELLA sulla RESTOCK_FIFO e chiudo le write-end
     *      dummy -> i receiver vedono EOF appena la FIFO si svuota; il thread
     *      restock esce appena legge la sentinella (anche se qualche supplier
     *      tiene ancora aperto il lato scrittura)
     *   2. join di receiver e restock  -> da qui NESSUN nuovo push su pending
     *      (i receiver bloccati in bq_push vengono comunque serviti dai picker,
     *      che sono ancora tutti attivi: nessun deadlock)
     *   3. SOLO ORA shutdown della pending -> i picker drenano cio' che resta
     *      e poi escono (vuota+shutdown)
     *   4. join dei picker             -> da qui nessun nuovo push su packaging
     *   5. shutdown della packaging    -> i packer drenano e poi escono
     *   6. join dei packer
     * L'ordine inverso (shutdown della pending PRIMA della join dei receiver)
     * sarebbe sbagliato: tutti i picker potrebbero uscire (coda vuota+shutdown)
     * mentre un receiver sta ancora accodando un ordine, che resterebbe
     * orfano e il suo client appeso fino al timeout. */

    RestockMsg stop_msg = { RESTOCK_STOP_ID, 0, 0 };
    if (write_all(restock_dummy_fd, &stop_msg, sizeof(stop_msg)) < 0)
        perror("[WAREHOUSE] write sentinella restock");

    close(orders_dummy_fd);
    close(restock_dummy_fd);

    for (int i = 0; i < num_receivers; i++) pthread_join(recv_th[i], NULL);
    pthread_join(restock_th, NULL);

    bq_shutdown(&pending);
    for (int i = 0; i < num_pickers;   i++) pthread_join(pick_th[i], NULL);

    bq_shutdown(&packaging);
    for (int i = 0; i < num_packers;   i++) pthread_join(pack_th[i], NULL);

    /* ---- cleanup risorse + IPC (spec 2.2.8: clean up FIFO/IPC) ---- */
    close(orders_fd);
    close(restock_fd);
    close(log_fd);
    /*UNLINK: = system call dietro rm, rimuove il nome non l 'oggetto,
     *se c'è qualche altro processo
     *con collegamento l'iNode rimane vivo*/
    unlink(ORDERS_FIFO);
    unlink(RESTOCK_FIFO);
    unlink(STATUS_FILE);

    bq_destroy(&pending);
    bq_destroy(&packaging);
    pthread_mutex_destroy(&inv.mutex);
    pthread_mutex_destroy(&log_mutex);
    pthread_mutex_destroy(&orders_read_mutex);
    pthread_mutex_destroy(&oid_mutex);

    free(recv_th);
    free(pick_th);
    free(pack_th);

    return EXIT_SUCCESS;
}