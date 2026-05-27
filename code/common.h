#ifndef COMMON_H
#define COMMON_H

/* ============================================================================
 * common.h — Definizioni condivise tra warehouse, supplier e gli script Bash.
 *
 * Questo file e' l'INTERFACCIA BINARIA del sistema. Tutti i processi che si
 * scambiano messaggi via FIFO devono includere ESATTAMENTE questa stessa
 * versione di common.h, altrimenti i sizeof(...) usati da read/write non
 * combaciano e i messaggi vengono interpretati in modo errato.
 *
 * Contiene:
 *   - path delle FIFO e dei file di sistema
 *   - error codes condivisi (stessi numeri usati da Bash via $?)
 *   - limiti di dimensione dei campi string
 *   - le tre struct wire-format scambiate sulle FIFO
 *
 * Non contiene: strutture interne al solo warehouse (Inventory, BoundedQueue,
 * Order), che vivono in warehouse.c perche' non escono dal processo.
 *
 * Riferimenti corso: Lab06 (IPC FIFO), Lab05 (FD e I/O).
 * ============================================================================ */

/* ====== 1. IPC PATHS AND SYSTEM FILES ====================================== */
#define ORDERS_FIFO          "/tmp/orders_fifo"   /* order.sh → warehouse    */
#define RESTOCK_FIFO        "/tmp/restock_fifo" /* supplier → warehouse    */
#define LOG_FILE             "orders.log"          /* scritto dai packer      */
#define STATUS_FILE          "/tmp/wh_status.tmp"  /* dump SIGUSR1 warehouse  */
#define WAREHOUSE_PID_FILE   "/tmp/warehouse.pid"  /* PID warehouse (bootstrap) */
#define SUPPLIERS_PID_FILE   "/tmp/suppliers.pid"  /* PID supplier (bootstrap)  */

/* ====== 2. ERROR CODES ===================================================== */
/* ═══════════════════════════════════════════════════════════════════════════
 * Error codes  (valori numerici condivisi con gli script Bash)
 *
 * In Bash usare le stesse costanti numeriche, es:
 *   ERR_OK=0  ERR_ITEM_NOT_FOUND=1  ERR_OUT_OF_STOCK=2 ...
 * ═══════════════════════════════════════════════════════════════════════════ */
#define ERR_OK              0
#define ERR_ITEM_NOT_FOUND  1   /* item_id non presente in inventario          */
#define ERR_OUT_OF_STOCK    2   /* item presente ma stock == 0                 */
#define ERR_INVALID_QTY     3   /* quantità <= 0                               */
#define ERR_QUEUE_FULL      4   /* bounded buffer pieno (non usato attivamente) */
#define ERR_IO              5   /* errore di I/O (file, FIFO, ecc.)            */
#define ERR_PARTIAL         6   /* consegna parziale: shipped < requested      */
#define ERR_SHUTTING_DOWN    7   /* warehouse non accetta nuovi ordini */
#define ERR_WAREHOUSE_DOWN   8   /* order.sh/manage.sh non trova warehouse */
#define ERR_TIMEOUT          9   /* attesa risposta IPC scaduta */
/* ====== 3. STRING FIELDS SIZE LIMITS =========================== */
/* ═══════════════════════════════════════════════════════════════════════════
 * Limiti di dimensione dei campi nelle struct wire-format CIPPA GAY
 * ═══════════════════════════════════════════════════════════════════════════ */
#define MAX_CLIENT_ID   64
#define MAX_DESC       128 // ci sta
#define MAX_CATEGORY    64 // ci sta
#define MAX_RESP_FIFO  256

/* ====== 4. SENTINEL VALUES ================================================= */
/* -------------------------------------------------------------------------
 * SENTINEL
 * Valori speciali usati durante lo shutdown per sbloccare thread in attesa.
 * Un receiver che legge un OrderMsg con item_id == SENTINEL_ITEM_ID sa
 * che deve uscire. Stesso principio per RestockMsg.
 * ------------------------------------------------------------------------- */
#define SENTINEL_ITEM_ID     -1
#define SENTINEL_SUPPLIER_ID -1

/* ====== 5. COMMON STRUCTS ================================================== */

//WIRE FORMAT : MESSAGE PASSING
/* Queste tre struct attraversano i confini di processo: vengono scritte come
 * blocchi BINARI (write/read di sizeof(struct)). Su FIFO Linux la write
 * < PIPE_BUF byte (>= 4096) e' atomica, quindi messaggi di writer
 * concorrenti non si mischiano (man 7 pipe). */
/* ═══════════════════════════════════════════════════════════════════════════
 * Struct wire-format: messaggi scambiati tra processi via FIFO
 *
 * ATTENZIONE: queste struct vengono scritte e lette come blocchi binari
 * (write/read di sizeof(struct)). Tutti i processi devono usare esattamente
 * la stessa definizione — per questo stanno in common.h.
 * ═══════════════════════════════════════════════════════════════════════════ */

/* order.sh → warehouse (su ORDERS_FIFO) */
//USATA DAL C HELPER DI order.sh
typedef struct {
    char client_id[MAX_CLIENT_ID];
    char resp_fifo[MAX_RESP_FIFO];  /* path della FIFO privata del client */
    int  item_id;
    int  quantity;
} OrderRequest;

/* warehouse → order.sh (su resp_fifo privata del client) */
//IL C HELPER di order.sh USA QUESTA STRUCT? si
typedef struct {
    int err_code;       /* ERR_* code */
    int qty_shipped;
    int qty_rejected;
} OrderResponse;

/* supplier → warehouse (su RESTOCK_FIFO) */
typedef struct {
    int supplier_id;
    int item_id;
    int quantity;
} RestockMsg;

#endif /* COMMON_H */
