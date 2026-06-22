#ifndef COMMON_H
#define COMMON_H

#include <sys/types.h>
#include <stddef.h>

/* ============================================================================
 * common.h -- Shared binary interface for all processes that
 * exchange messages over the FIFOs: warehouse, supplier, order_helper and
 * manage_restock_helper. Every process must include this
 * same header so that sizeof(struct) matches on both ends of a read/write.
 *
 * Holds: IPC paths, shared error codes, string-field limits, the three
 * wire-format structs and the prototypes of the shared helpers (common.c).
 * ============================================================================ */

/* ====== IPC PATHS AND SYSTEM FILES ====================================== */
#define ORDERS_FIFO          "/tmp/orders_fifo"    /* order.sh  -> warehouse    */
#define RESTOCK_FIFO         "/tmp/restock_fifo"   /* supplier  -> warehouse    */
#define RESP_FIFO_TEMPLATE   "/tmp/order_resp_%d"  /* warehouse -> client (%d = PID) */
#define LOG_FILE             "orders.log"          /* written by packers        */
#define STATUS_FILE          "/tmp/wh_status.tmp"  /* SIGUSR1 status dump        */
#define WAREHOUSE_PID_FILE   "/tmp/warehouse.pid"  /* PID warehouse (bootstrap)  */
#define SUPPLIERS_PID_FILE   "/tmp/suppliers.pid"  /* PID supplier  (bootstrap)  */

/* ====== ERROR CODES ===================================================== */
#define ERR_OK              0   /* success                                      */
#define ERR_ITEM_NOT_FOUND  1   /* item_id not in inventory                     */
#define ERR_OUT_OF_STOCK    2   /* item present but stock == 0                  */
#define ERR_INVALID_QTY     3   /* quantity <= 0                                */
#define ERR_IO              4   /* I/O error (file, FIFO, ...)                  */
#define ERR_PARTIAL_FILL         5   /* partial delivery: shipped < requested        */
#define ERR_WAREHOUSE_DOWN  6   /* warehouse not found by order.sh/manage.sh    */
#define ERR_TIMEOUT         7   /* IPC response wait expired                     */
#define ERR_USAGE          8   /* wrong arguments (scripts and C helpers)      */

/* ======  STRING FIELD SIZE LIMITS ======================================== */
#define MAX_CLIENT_ID   64
#define MAX_DESC       128
#define MAX_CATEGORY    64
#define MAX_RESP_FIFO  256

/* ======  SPECIAL VALUES ================================================== */
#define MANUAL_RESTOCK_SUPPLIER_ID  0

/* ====== 5. WIRE-FORMAT STRUCTS ============================================= */
/* Exchanged as raw binary blocks (write/read of sizeof(struct)). All three are
 * well below PIPE_BUF, so each write on a FIFO is atomic (man 7 pipe). */

/* order_helper -> warehouse (on ORDERS_FIFO) */
typedef struct {
    char client_id[MAX_CLIENT_ID];
    char resp_fifo[MAX_RESP_FIFO];  /* client's private response FIFO */
    int  item_id;
    int  quantity;
} OrderRequest;

/* warehouse -> order_helper (on the client's private resp_fifo) */
typedef struct {
    int status;       /* ERR_* code */
    int item_id;
    int qty_requested;
    int qty_shipped;
    int qty_rejected;
} OrderResponse;

/* supplier / manage_restock_helper -> warehouse (on RESTOCK_FIFO) */
typedef struct {
    int supplier_id;
    int item_id;
    int quantity;
} RestockMsg;

/* ====== SHARED HELPERS (defined in common.c) =========================== */

/* Install a handler for 'sig' via sigaction, WITHOUT SA_RESTART. */
void setup_handler(int sig, void (*handler)(int));

/* "Full" write: repeats until all 'len' bytes are out, retrying on EINTR.
 * Returns len, or -1 with errno set.*/
ssize_t write_all(int fd, const void *buf, size_t len);

/* Open 'path' as a FIFO ready for a reader: mkfifo, read-end
 * opened O_NONBLOCK then cleared, plus a dummy write-end so read() never sees a
 * spurious EOF. Returns 0 and fills read_fd and dummy_write_fd; on error
 * returns -1 with errno preserved and prints nothing. */
int open_fifo_r_dw(const char *path, mode_t mode, int *read_fd, int *dummy_write_fd);

/* Read one line from fd, byte by byte. Used for CSV/.conf loaded once
 * at startup. Returns bytes read, 0 = EOF, -1 = error.*/
ssize_t read_line_from_fd(int fd, char *buf, size_t size);
#endif
