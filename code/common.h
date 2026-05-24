#ifndef COMMON_H
#define COMMON_H

/* ═══════════════════════════════════════════════════════════════════════════
 * common.h — Definizioni condivise tra warehouse, supplier, order.sh
 *
 * Contiene: path IPC, error codes, limiti di dimensione, struct wire-format.
 * NON contiene: strutture interne al warehouse (Inventory, Order, BoundedQueue)
 *               che non vengono mai scambiate tra processi diversi.
 * ═══════════════════════════════════════════════════════════════════════════ */

/* ═══════════════════════════════════════════════════════════════════════════
 * Path IPC e file di sistema
 *
 * I path delle FIFO sono definiti qui in common.h invece di usare getenv()
 * perché sono fissi e condivisi da tutti i processi (warehouse, supplier,
 * order.sh). getenv() aggiungerebbe complessità senza benefici: richiederebbe
 * export nel bootstrap e causerebbe crash non ovvi se la variabile fosse assente.
 * ═══════════════════════════════════════════════════════════════════════════ */
#define ORDERS_FIFO          "/tmp/orders_queue"   /* order.sh → warehouse    */
#define SUPPLIER_FIFO        "/tmp/supplier_queue" /* supplier → warehouse    */
#define LOG_FILE             "orders.log"          /* scritto dai packer      */
#define STATUS_FILE          "/tmp/wh_status.tmp"  /* dump SIGUSR1 warehouse  */
#define WAREHOUSE_PID_FILE   "/tmp/warehouse.pid"  /* PID warehouse (bootstrap) */
#define SUPPLIERS_PID_FILE   "/tmp/suppliers.pid"  /* PID supplier (bootstrap)  */

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

/* ═══════════════════════════════════════════════════════════════════════════
 * Limiti di dimensione dei campi nelle struct wire-format CIPPA GAY
 * ═══════════════════════════════════════════════════════════════════════════ */
#define MAX_CLIENT_ID   64
#define MAX_DESC       128 // ci sta
#define MAX_CATEGORY    64 // ci sta
#define MAX_RESP_FIFO  256

/* -------------------------------------------------------------------------
 * SENTINEL
 * Valori speciali usati durante lo shutdown per sbloccare thread in attesa.
 * Un receiver che legge un OrderMsg con item_id == SENTINEL_ITEM_ID sa
 * che deve uscire. Stesso principio per RestockMsg.
 * ------------------------------------------------------------------------- */
#define SENTINEL_ITEM_ID     -1
#define SENTINEL_SUPPLIER_ID -1
/* ═══════════════════════════════════════════════════════════════════════════
 * Item — struttura base dell'inventario
 * (usata dal warehouse; definita qui perché leggibile anche da altri moduli)
 * ═══════════════════════════════════════════════════════════════════════════ */
typedef struct {
    int  item_id;
    char description[MAX_DESC];
    char category[MAX_CATEGORY];
    int  stock;
} Item;

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
