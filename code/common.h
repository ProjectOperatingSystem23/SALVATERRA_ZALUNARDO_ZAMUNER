#ifndef COMMON_H
#define COMMON_H

#include <sys/types.h>  /* ssize_t, mode_t (per i prototipi degli helper) */
#include <stddef.h>     /* size_t */

/* ============================================================================
 * common.h — Definizioni condivise tra warehouse, supplier, order_client,
 *            restock_client e gli script Bash.
 *
 * Questo file e' l'INTERFACCIA BINARIA del sistema. Tutti i processi che si
 * scambiano messaggi via FIFO devono includere ESATTAMENTE questa stessa
 * versione di common.h, altrimenti i sizeof(...) usati da read/write non
 * combaciano e i messaggi vengono interpretati in modo errato.
 *
 * Contiene:
 *   - path delle FIFO e dei file di sistema
 *   - error codes condivisi (stessi numeri usati da Bash via $?)
 *   - limiti di dimensione dei campi stringa
 *   - le tre struct wire-format scambiate sulle FIFO
 *
 * Non contiene: strutture interne al solo warehouse (Inventory, BoundedQueue,
 * Order), che vivono in warehouse.c perche' non escono dal processo.
 *
 * Riferimenti corso: Lab06 (IPC FIFO), Lab05 (FD e I/O).
 * ============================================================================ */

/* ====== 1. IPC PATHS AND SYSTEM FILES ====================================== */
#define ORDERS_FIFO          "/tmp/orders_fifo"    /* order.sh  -> warehouse    */
#define RESTOCK_FIFO         "/tmp/restock_fifo"   /* supplier  -> warehouse    */
#define RESP_FIFO_TEMPLATE   "/tmp/order_resp_%d"  /* warehouse -> order_client
                                                    * (una FIFO privata per
                                                    * client, %d = PID)         */
#define LOG_FILE             "orders.log"          /* scritto dai packer        */
#define STATUS_FILE          "/tmp/wh_status.tmp"  /* dump SIGUSR1 warehouse    */
#define WAREHOUSE_PID_FILE   "/tmp/warehouse.pid"  /* PID warehouse (bootstrap) */
#define SUPPLIERS_PID_FILE   "/tmp/suppliers.pid"  /* PID supplier (bootstrap)  */

/* ====== 2. ERROR CODES ===================================================== */
/* Valori numerici condivisi con gli script Bash, che definiscono le stesse
 * costanti (ERR_OK=0, ERR_ITEM_NOT_FOUND=1, ...) e le confrontano con $?.
 * La spec (2.2.9) richiede che C e Bash usino gli stessi valori, non che il
 * file sia fisicamente condiviso. */

/*TODO: VEDERE QUANDO VENGONO USATI*/
/* Dove vengono usati:
 *   - OrderResponse.status (warehouse -> client): OK/NOT_FOUND/OUT_OF_STOCK/
 *     INVALID_QTY/PARTIAL;
 *   - exit code dei processi C (supplier, order_client, restock_client):
 *     OK/USAGE/IO/WAREHOUSE_DOWN;
 *   - exit code degli script Bash (order.sh, manage.sh), che li ricopiano. */

#define ERR_OK              0   /* successo                                     */
#define ERR_ITEM_NOT_FOUND  1   /* item_id non presente in inventario           */
#define ERR_OUT_OF_STOCK    2   /* item presente ma stock == 0                  */
#define ERR_INVALID_QTY     3   /* quantita' <= 0                               */
/*noi abbiamo il consume bloccante sulle code, quindi non c'è bisogno di un codice di errore CODA PIENA*/
#define ERR_IO              4   /* errore di I/O (file, FIFO, ecc.)             */
#define ERR_PARTIAL         5   /* consegna parziale: shipped < requested       */
#define ERR_WAREHOUSE_DOWN  6   /* order.sh/manage.sh non trova il warehouse    */
#define ERR_TIMEOUT         7   /* attesa risposta IPC scaduta                  */
#define ERR_USAGE          8   /* argomenti errati (script e helper C)         */

/* ====== 3. STRING FIELD SIZE LIMITS ======================================== */
#define MAX_CLIENT_ID   64
#define MAX_DESC       128
#define MAX_CATEGORY    64
#define MAX_RESP_FIFO  256

/* ====== 4. SPECIAL VALUES ================================================== */
/*
 * MANUAL_RESTOCK_SUPPLIER_ID
 * Usato da manage.sh (tramite l'helper restock_client) per inviare un restock
 * manuale via RESTOCK_FIFO, riutilizzando la stessa struct RestockMsg dei
 * supplier reali. Il warehouse distingue:
 *   supplier_id == 0 -> restock manuale,
 *   supplier_id >= 1 -> supplier reale.
 *
 * NB: il warehouse rifiuta (ERR_ITEM_NOT_FOUND) qualsiasi ordine con
 * item_id <= 0 e qualsiasi restock con item_id o quantity <= 0: nessun
 * valore "speciale" puo' essere iniettato dall'esterno.
 */
#define MANUAL_RESTOCK_SUPPLIER_ID  0

/* ====== 5. WIRE-FORMAT STRUCTS ============================================= */
/* Queste tre struct attraversano i confini di processo: vengono scritte e
 * lette come blocchi BINARI (write/read di sizeof(struct)). Su Linux una
 * write su FIFO di dimensione < PIPE_BUF (>= 4096 byte) e' ATOMICA, quindi i
 * messaggi di writer concorrenti non si mischiano (man 7 pipe). Tutte e tre
 * le struct stanno ampiamente sotto questo limite. */

/* order_client -> warehouse (su ORDERS_FIFO) */
typedef struct {
    char client_id[MAX_CLIENT_ID];
    char resp_fifo[MAX_RESP_FIFO];  /* path della FIFO privata del client */
    int  item_id;
    int  quantity;
} OrderRequest;

/* warehouse -> order_client (sulla resp_fifo privata del client) */
typedef struct {
    int status;       /* ERR_* code */
    int item_id;
    int qty_requested;
    int qty_shipped;
    int qty_rejected;
} OrderResponse;

/* supplier / c helper -> warehouse (su RESTOCK_FIFO) */
typedef struct {
    int supplier_id;
    int item_id;
    int quantity;
} RestockMsg;

/* ====== 6. HELPER CONDIVISI (definiti in common.c) ========================= */
/* Funzioni identiche usate da warehouse, supplier e order_client: definite UNA
 * sola volta in common.c e linkate in tutti gli eseguibili (DRY, niente
 * divergenze). I flag dei segnali (g_stop, g_timed_out, ...) restano invece
 * locali a ciascun processo perche' sono stato per-processo. */

/* Installa un handler per 'sig' con sigaction, SENZA SA_RESTART: le syscall
 * lente vengono interrotte dai segnali (serve per alarm/sleep). (Lab03) */
void setup_handler(int sig, void (*fn)(int));

/* write "completa": ripete la write finche' tutti i 'len' byte sono usciti,
 * riprovando su EINTR. Ritorna len, oppure -1 con errno settato. (Lab05) */
ssize_t write_all(int fd, const void *buf, size_t len);

/* Apre 'path' come FIFO pronta all'uso lato lettore:
 *   - mkfifo(path, mode) idempotente (tollera EEXIST);
 *   - read-end in O_NONBLOCK (la open non blocca senza writer);
 *   - una write-end "dummy" -> c'e' sempre >=1 writer: niente EOF spurio;
 *   - fcntl() toglie O_NONBLOCK dal read-end (read successive bloccanti).
 * Ritorna 0 e riempie *read_fd e *dummy_write_fd; su errore ritorna -1 lasciando
 * errno valido e NON stampa nulla (il messaggio lo decide il chiamante). (Lab06) */
int open_fifo_rw(const char *path, mode_t mode, int *read_fd, int *dummy_write_fd);

/* ── lettura di una riga da fd, byte per byte (Lab05) ──────────────────────
 * Con i soli fd (niente stdio) non c'e' una "readline" pronta: leggere 1 byte
 * alla volta e' la soluzione piu' semplice e corretta. Usata per CSV/.conf,
 * caricati una sola volta all'avvio: l'inefficienza e' irrilevante.
 * Ritorna i byte letti, 0 = EOF, -1 = errore. */
ssize_t fd_read_line(int fd, char *buf, size_t size);
#endif /* COMMON_H */
