/* ============================================================================
 * warehouse.c -- Warehouse process of the Fulfillment Center
 *
 * Usage:
 *   ./warehouse <num_receivers> <num_pickers> <num_packers>
 *               <queue_capacity> <inventory.csv>
 *
 * Loads the inventory, opens the IPC FIFOs and runs four thread pools that
 * cooperate over two bounded buffers:
 *   order.sh --FIFO--> [Receiver]* --pending--> [Picker]* --packaging--> [Packer]* --> orders.log
 *   supplier --FIFO--> [Restock] -> inventory
 * ============================================================================ */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <pthread.h>
#include <signal.h>
#include <errno.h>
#include <time.h>
#include "common.h"

/* ====== Internal constants ================================================= */
#define MAX_INV_SIZE  1024   /* upper bound on items in the CSV */
#define LINE_BUF       512   /* one log / status line          */
#define RESTOCK_STOP_ID -1   /* sentinel supplier_id: sent ONLY by main on its
                              * dummy write-end to stop the restock thread */

#define STATUS_FILE_TMP STATUS_FILE ".inprogress"

/* ====== Internal data structures (process-local: not in common.h) ========= */

/* Order state along the pipeline */
typedef enum {
    ORDER_RECEIVED,
    ORDER_PICKING,
    ORDER_PACKING,
    ORDER_SHIPPED,
    ORDER_REJECTED
} OrderStatus;

/* One inventory item. */
typedef struct {
    int  item_id;
    char description[MAX_DESC];
    char category[MAX_CATEGORY];
    int  stock;
} Item;

/* The inventory: item array + one mutex guarding the stock. */
typedef struct {
    Item             items[MAX_INV_SIZE]; /* static array: no malloc/free */
    int              count;
    pthread_mutex_t  mutex;
} Inventory;

/* A live order inside the warehouse: client request + internal tracking. */
typedef struct {
    OrderRequest request;
    int          order_id;
    int          qty_shipped;
    int          qty_rejected;
    int          error_code;
    OrderStatus  status;
} Order;

/* Circular bounded buffer: mutex + not_full/not_empty condvars + shutdown flag. */
typedef struct {
    Order           *buffer;
    int              head, tail, count, capacity;
    pthread_mutex_t  mutex;
    pthread_cond_t   not_full;
    pthread_cond_t   not_empty;
    int              shutdown;
} BoundedQueue;

/* ---- Thread-argument structs: references only, owned by main ------- */
typedef struct {
    int              orders_fd;          /* ORDERS_FIFO read-end*/
    pthread_mutex_t *orders_read_mutex;  /* serialises the read across receivers */
    Inventory       *inv;
    BoundedQueue    *pending;
    int             *next_order_id;      /* shared order_id counter*/
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
    int        restock_fd;               /* RESTOCK_FIFO read-end*/
    Inventory *inv;
} RestockArgs;

/* ====== Global variables ============================= */
static volatile sig_atomic_t shutdown_flag    = 0;   /* SIGTERM / SIGINT */
static volatile sig_atomic_t dump_status_flag = 0;   /* SIGUSR1          */

/* ====== fd I/O =========================================== */

/* Full read with EINTR handling. Returns bytes read or -1 on error. */
static ssize_t read_all(int fd, void *buf, size_t len)
{
    size_t done = 0;
    while (done < len) {
        ssize_t n = read(fd, (char *)buf + done, len - done);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) break;
        done += (size_t)n;
    }
    return (ssize_t)done;
}

/* ====== Inventory: CSV load via fd + synchronised access =================== */

/* Extract one comma-separated field from cursor *pp into dst,
 * then advance *pp past the field and its comma. */
static void csv_field(char **pp, char *dst, int dst_size)
{
    char *p = *pp;
    int   i = 0;
    while (*p && *p != ',' && *p != '\n' && *p != '\r')
    {
        if (i < dst_size-1)
            dst[i++] = *p;
        p++;
    }
    dst[i] = '\0';
    if (*p == ',') p++;              /* skip separator; stop at \n or \r  */
    *pp = p;
}

/* Load the inventory CSV. */
static int inventory_load(Inventory *inv, const char *path)
{
    int fd = open(path, O_RDONLY);
    if (fd < 0) {
        fprintf(stderr, "[WAREHOUSE] open inventory '%s': %s\n",
                path, strerror(errno));
        return ERR_IO;
    }

    char line[512];
    /* skip header */
    if (read_line_from_fd(fd, line, sizeof(line)) <= 0) {
        fprintf(stderr, "[WAREHOUSE] inventory '%s' empty\n", path);
        close(fd);
        return ERR_IO;
    }

    inv->count = 0;
    while (read_line_from_fd(fd, line, sizeof(line)) > 0) {
        if (line[0] == '\r' || line[0] == '\n' || line[0] == '\0') continue;
        if (inv->count >= MAX_INV_SIZE) {
            fprintf(stderr, "[WAREHOUSE] inventory truncated to %d items\n",
                    MAX_INV_SIZE);
            break;
        }
        Item *it = &inv->items[inv->count];
        char *p  = line;
        char  tmp[32];

        csv_field(&p, tmp, sizeof(tmp));
        it->item_id = atoi(tmp);
        if (it->item_id <= 0) continue;          /* malformed row */

        csv_field(&p, it->description, sizeof(it->description));
        csv_field(&p, it->category,    sizeof(it->category));

        csv_field(&p, tmp, sizeof(tmp));
        it->stock = atoi(tmp);
        if (it->stock < 0) it->stock = 0;        /* defensive: never stock < 0 */

        inv->count++;
    }

    close(fd);

    if (inv->count == 0) {
        fprintf(stderr, "[WAREHOUSE] no valid items in '%s'\n", path);
        return ERR_IO;
    }
    return ERR_OK;
}

/* Find item by id. Caller MUST already hold inv->mutex. */
static Item *inventory_find_locked(Inventory *inv, int item_id)
{
    for (int i = 0; i < inv->count; i++)
        if (inv->items[i].item_id == item_id)
            return &inv->items[i];
    return NULL;
}

/* Atomic stock decrement (caller holds inv->mutex). Returns units shipped
 * and sets *err. */
static int inventory_decrement_locked(Inventory *inv, int item_id, int qty,
                                      int *err)
{
    Item *it = inventory_find_locked(inv, item_id);
    if (!it)            { *err = ERR_ITEM_NOT_FOUND; return 0; }
    if (it->stock <= 0) { *err = ERR_OUT_OF_STOCK;   return 0; }

    int ship = (it->stock >= qty) ? qty : it->stock;  /* partial fill */
    it->stock -= ship;
    *err = (ship < qty) ? ERR_PARTIAL_FILL : ERR_OK;
    return ship;
}

/* Stock increment, grabs inv->mutex itself. Returns new stock, or -1 if the
 * item does not exist. */
static int inventory_increment(Inventory *inv, int item_id, int qty)
{
    int newstock = -1;
    pthread_mutex_lock(&inv->mutex);
    Item *it = inventory_find_locked(inv, item_id);
    if (it) { it->stock += qty; newstock = it->stock; }
    pthread_mutex_unlock(&inv->mutex);
    return newstock;
}

/* ====== Bounded buffer ================ */
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

/* Insert an order; blocks while full. Returns 0, or -1 if the queue is
 * in shutdown AND full. The while-loop guards against spurious wakeups. */
static int bq_produce(BoundedQueue *q, const Order *o)
{
    pthread_mutex_lock(&q->mutex);
    while (q->count == q->capacity && !q->shutdown)
        pthread_cond_wait(&q->not_full, &q->mutex);
    if (q->count == q->capacity && q->shutdown) {
        pthread_cond_signal(&q->not_full);   /* chained wake (see bq_consume) */
        pthread_mutex_unlock(&q->mutex);
        return -1;
    }
    q->buffer[q->tail] = *o;
    q->tail = (q->tail + 1) % q->capacity;
    q->count++;
    pthread_cond_signal(&q->not_empty);      /* wake one consumer */
    pthread_mutex_unlock(&q->mutex);
    return 0;
}

/* Remove an order; blocks while empty. Returns 0, or -1 if empty AND in
 * shutdown. In-flight orders are drained first. */
static int bq_consume(BoundedQueue *q, Order *out)
{
    pthread_mutex_lock(&q->mutex);
    while (q->count == 0 && !q->shutdown)
        pthread_cond_wait(&q->not_empty, &q->mutex);
    if (q->count == 0) {
        /* Chained wake: wake the next waiter, which re-checks empty+shutdown and
         * re-signals. One signal in bq_shutdown thus wakes everyone. */
        pthread_cond_signal(&q->not_empty);
        pthread_mutex_unlock(&q->mutex);
        return -1;
    }
    *out = q->buffer[q->head];
    q->head = (q->head + 1) % q->capacity;
    q->count--;
    pthread_cond_signal(&q->not_full);       /* wake one producer */
    pthread_mutex_unlock(&q->mutex);
    return 0;
}

/* Raise shutdown and wake all threads waiting on either condvar. */
static void bq_shutdown(BoundedQueue *q)
{
    pthread_mutex_lock(&q->mutex);
    q->shutdown = 1;
    pthread_cond_signal(&q->not_full);
    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->mutex);
}

/* Current element count, synchronised for the status dump. */
static int bq_size(BoundedQueue *q)
{
    pthread_mutex_lock(&q->mutex);
    int c = q->count;
    pthread_mutex_unlock(&q->mutex);
    return c;
}

/* ====== Logging to orders.log via fd ==============================
 * Format (pipe-separated):
 *   timestamp|order_id|client_id|item_id|qty_req|qty_shipped|qty_rejected|STATUS
 *   STATUS in { SHIPPED, PARTIAL, REJECTED }
 * ========================================================================== */
static void log_order(int log_fd, pthread_mutex_t *log_mutex, const Order *o)
{
    char tbuf[32];
    time_t now = time(NULL);
    snprintf(tbuf, sizeof(tbuf), "%ld", (long)now);

    const char *st = "SHIPPED";
    if      (o->status == ORDER_REJECTED)  st = "REJECTED";
    else if (o->error_code == ERR_PARTIAL_FILL) st = "PARTIAL";

    char line[LINE_BUF];
    int n = snprintf(line, sizeof(line), "%s|%d|%s|%d|%d|%d|%d|%s\n",
                     tbuf, o->order_id, o->request.client_id, o->request.item_id,
                     o->request.quantity, o->qty_shipped, o->qty_rejected, st);
    if (n <= 0) return;

    pthread_mutex_lock(log_mutex);
    ssize_t w = write_all(log_fd, line, (size_t)n);
    int e = errno;
    pthread_mutex_unlock(log_mutex);
    if (w < 0)                           /*log writes must be checked */
        fprintf(stderr, "[WAREHOUSE] write to %s failed: %s\n", LOG_FILE, strerror(e));
}

/* ====== Reply to the client on its private FIFO  ====================
 * Opened O_WRONLY|O_NONBLOCK so a dead client (ENXIO) cannot hang a packer.
 * The reply stays binary (OrderResponse); order_helper displays it for the user.
 * ========================================================================== */
static void send_response(const char *resp_fifo, const OrderResponse *resp)
{
    if (resp_fifo == NULL || resp_fifo[0] == '\0') return;
    int fd = open(resp_fifo, O_WRONLY | O_NONBLOCK);
    if (fd < 0) {
        fprintf(stderr, "[WAREHOUSE] undeliverable response for '%s': %s "
                        "(client terminated?)\n", resp_fifo, strerror(errno));
        return;
    }
    if (write_all(fd, resp, sizeof(*resp)) < 0)
        fprintf(stderr, "[WAREHOUSE] write response to '%s': %s\n",
                resp_fifo, strerror(errno));
    close(fd);
}

/* ====== Misc ============================================================== */

/* Sleep a random 1-3 s to simulate physical work(spec 2.2.7). */
static void rand_sleep_1_3(void)
{
    sleep(1 + rand() % 3);
}

/* ====== Signal handlers (Lab03): set a flag only ========================== */
static void handle_shutdown(int sig) { (void)sig; shutdown_flag    = 1; }
static void handle_status_dump    (int sig) { (void)sig; dump_status_flag = 1; }

/* ====== Status dump to STATUS_FILE (for "manage.sh status") ================*/
static void write_status_file(Inventory *inv, BoundedQueue *pending,
                           BoundedQueue *packaging, int n_rec, int n_pick, int n_pack)
{
    int fd = open(STATUS_FILE_TMP, O_WRONLY | O_CREAT | O_TRUNC, 0644);
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
                 n_rec, n_pick, n_pack);
    if (n > 0) write_all(fd, buf, (size_t)n);
    n = snprintf(buf, sizeof(buf), "PENDING_QUEUE=%d/%d\n",
                 bq_size(pending), pending->capacity);
    if (n > 0) write_all(fd, buf, (size_t)n);
    n = snprintf(buf, sizeof(buf), "PACKAGING_QUEUE=%d/%d\n",
                 bq_size(packaging), packaging->capacity);
    if (n > 0) write_all(fd, buf, (size_t)n);

    /* Snapshot the inventory under lock, then write outside the lock. */
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

    /* Atomic rename: STATUS_FILE becomes the complete dump in one step. */
    if (rename(STATUS_FILE_TMP, STATUS_FILE) != 0) {
        fprintf(stderr, "[WAREHOUSE] rename status '%s' -> '%s': %s\n",
                STATUS_FILE_TMP, STATUS_FILE, strerror(errno));
        unlink(STATUS_FILE_TMP);
    }
}

/* ====== THREAD: Order Receiver (producer) ====================
 * Many receivers read the SAME FIFO; a mutex on the read gives each exactly one
 * full OrderRequest (writes are atomic, < PIPE_BUF).
 * ========================================================================== */
static void log_rejection(int log_fd, pthread_mutex_t *log_mutex,
                          const OrderRequest *req, int oid, int err)
{
    Order o;
    memset(&o, 0, sizeof(o));
    o.request = *req;
    o.order_id     = oid;
    o.status       = ORDER_REJECTED;
    o.error_code   = err;
    o.qty_rejected = req->quantity;
    log_order(log_fd, log_mutex, &o);
}

static void *receiver_thread(void *arg)
{
    ReceiverArgs *a = (ReceiverArgs *)arg;

    while (!shutdown_flag) {
        OrderRequest req;
        /* One receiver reads one whole message at a time. */
        pthread_mutex_lock(a->orders_read_mutex);
        ssize_t n = read_all(a->orders_fd, &req, sizeof(req));
        pthread_mutex_unlock(a->orders_read_mutex);

        if (n == 0) break;                       /* EOF: dummy write-end closed */
        if (n != (ssize_t)sizeof(req)) {         /* error / truncated message   */
            if (n < 0) perror("[RECEIVER] read");
            if (shutdown_flag) break;
            continue;
        }
        /* Terminate strings coming from an external process. */
        req.client_id[MAX_CLIENT_ID - 1] = '\0';
        req.resp_fifo[MAX_RESP_FIFO - 1] = '\0';
        pthread_mutex_lock(a->oid_mutex);
        int order_id = (*a->next_order_id)++;
        pthread_mutex_unlock(a->oid_mutex);

        OrderResponse resp;
        memset(&resp, 0, sizeof(resp));
        resp.item_id = req.item_id;
        resp.qty_requested = req.quantity;

        /* --- Validation (spec 2.2.4 / 2.2.10) --- */
        if (req.quantity <= 0) {
            resp.status = ERR_INVALID_QTY; resp.qty_rejected = req.quantity;
            send_response(req.resp_fifo, &resp);
            log_rejection(a->log_fd, a->log_mutex, &req, order_id, ERR_INVALID_QTY);
            continue;
        }
        if (req.item_id <= 0) {
            resp.status = ERR_ITEM_NOT_FOUND; resp.qty_rejected = req.quantity;
            send_response(req.resp_fifo, &resp);
            log_rejection(a->log_fd, a->log_mutex, &req, order_id, ERR_ITEM_NOT_FOUND);
            continue;
        }

        /* stock peek; the definitive stock check is in the
         * picker, where the race is resolved. */
        pthread_mutex_lock(&a->inv->mutex);
        Item *it = inventory_find_locked(a->inv, req.item_id);
        int exists = (it != NULL);
        int in_stock = exists && it->stock > 0;
        pthread_mutex_unlock(&a->inv->mutex);

        if (!exists) {
            resp.status = ERR_ITEM_NOT_FOUND; resp.qty_rejected = req.quantity;
            send_response(req.resp_fifo, &resp);
            log_rejection(a->log_fd, a->log_mutex, &req, order_id, ERR_ITEM_NOT_FOUND);
            continue;
        }
        if (!in_stock) {
            resp.status = ERR_OUT_OF_STOCK; resp.qty_rejected = req.quantity;
            send_response(req.resp_fifo, &resp);
            log_rejection(a->log_fd, a->log_mutex, &req, order_id, ERR_OUT_OF_STOCK);
            continue;
        }

        Order o;
        memset(&o, 0, sizeof(o));
        o.request  = req;
        o.order_id = order_id;
        o.status   = ORDER_RECEIVED;
        if (bq_produce(a->pending, &o) != 0) break;   /* only if queue in shutdown */
    }
    return NULL;
}

/* ====== THREAD: Picker (consumes pending, produces packaging) == */
static void *picker_thread(void *arg)
{
    PickerArgs *a = (PickerArgs *)arg;

    for (;;) {
        Order o;
        if (bq_consume(a->pending, &o) != 0) break;       /* empty + shutdown -> exit */

        o.status = ORDER_PICKING;

        rand_sleep_1_3();                             /* pick time */

        /* Atomic stock decrement under inv->mutex. */
        int err;
        pthread_mutex_lock(&a->inv->mutex);
        int shipped = inventory_decrement_locked(a->inv, o.request.item_id,
                                                 o.request.quantity, &err);
        pthread_mutex_unlock(&a->inv->mutex);

        o.qty_shipped  = shipped;
        o.qty_rejected = o.request.quantity - shipped;
        o.error_code   = err;

        if (shipped == 0) {        /* ran out of item between validation and pick */
            o.status = ORDER_REJECTED;
            OrderResponse resp = { err, o.request.item_id, o.request.quantity,
                                   0, o.request.quantity };
            send_response(o.request.resp_fifo, &resp);
            log_order(a->log_fd, a->log_mutex, &o);
            continue;
        }

        /* Forward to packaging. */
        o.status = ORDER_PACKING;
        if (bq_produce(a->packaging, &o) != 0) break;    /* only if queue in shutdown */
    }
    return NULL;
}

/* ====== THREAD: Packer (consumes packaging) ================== */
static void *packer_thread(void *arg)
{
    PackerArgs *a = (PackerArgs *)arg;

    for (;;) {
        Order o;
        if (bq_consume(a->packaging, &o) != 0) break;

        rand_sleep_1_3();                             /* packing time */

        o.status = ORDER_SHIPPED;
        int status = (o.error_code == ERR_PARTIAL_FILL) ? ERR_PARTIAL_FILL : ERR_OK;
        OrderResponse resp = { status, o.request.item_id, o.request.quantity,
                               o.qty_shipped, o.qty_rejected };
        send_response(o.request.resp_fifo, &resp);
        log_order(a->log_fd, a->log_mutex, &o);
    }
    return NULL;
}

/* ====== THREAD: Restock listener (consumes RESTOCK_FIFO) =====
 * Runs concurrently with order processing (shares inv->mutex with pickers).
 * Suppliers are long-lived, so EOF may never come at shutdown: the thread also
 * exits on the sentinel (supplier_id == RESTOCK_STOP_ID) written by main.
 * ========================================================================== */
static void *restock_thread(void *arg)
{
    RestockArgs *a = (RestockArgs *)arg;

    for (;;) {
        RestockMsg msg;
        ssize_t n = read_all(a->restock_fd, &msg, sizeof(msg));

        if (n == 0) break;                          /* EOF: no writer */
        if (n != (ssize_t)sizeof(msg)) {
            if (n < 0) perror("[RESTOCK] read");
            if (shutdown_flag) break;
            continue;
        }
        if (msg.supplier_id == RESTOCK_STOP_ID)     /* sentinel from main */
            break;
        if (msg.item_id <= 0 || msg.quantity <= 0) {
            fprintf(stderr, "[RESTOCK] invalid message (item=%d qty=%d)\n",
                    msg.item_id, msg.quantity);
            continue;
        }

        int new_stock = inventory_increment(a->inv, msg.item_id, msg.quantity);
        if (new_stock < 0)
            fprintf(stderr, "[RESTOCK] item_id=%d not found.\n", msg.item_id);
    }
    return NULL;
}

/* ====== MAIN ============================================================== */
int main(int argc, char *argv[])
{
    /* ---- argument parsing + validation ---- */
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
        fprintf(stderr, "[WAREHOUSE] every number parameters must be > 0\n");
        return EXIT_FAILURE;
    }
    srand((unsigned)time(NULL) ^ (unsigned)getpid());

    /* ---- inventory ---- */
    Inventory inv;
    if (pthread_mutex_init(&inv.mutex, NULL) != 0) {
        fprintf(stderr, "[WAREHOUSE] inventory mutex init failed.\n");
        return EXIT_FAILURE;
    }
    if (inventory_load(&inv, inv_path) != ERR_OK)
        return EXIT_FAILURE;

    /* ---- log file---- */
    int log_fd = open(LOG_FILE, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (log_fd < 0) {
        fprintf(stderr, "[WAREHOUSE] open log '%s': %s\n", LOG_FILE, strerror(errno));
        return EXIT_FAILURE;
    }
    pthread_mutex_t log_mutex;
    if (pthread_mutex_init(&log_mutex, NULL) != 0) {
        fprintf(stderr, "[WAREHOUSE] log mutex init failed.\n");
        return EXIT_FAILURE;
    }

    /* ---- bounded queues ---- */
    BoundedQueue pending, packaging;
    if (bq_init(&pending, queue_cap) != ERR_OK ||
        bq_init(&packaging, queue_cap) != ERR_OK) {
        fprintf(stderr, "[WAREHOUSE] code init failed.\n");
        return EXIT_FAILURE;
    }

    /* ---- FIFOs ---- */
    int orders_fd, orders_dummy_fd, restock_fd, restock_dummy_fd;
    if (open_fifo_r_dw(ORDERS_FIFO,  0666, &orders_fd,  &orders_dummy_fd)  != 0) {
        fprintf(stderr, "[WAREHOUSE] init FIFO '%s': %s\n", ORDERS_FIFO, strerror(errno));
        return EXIT_FAILURE;
    }
    if (open_fifo_r_dw(RESTOCK_FIFO, 0666, &restock_fd, &restock_dummy_fd) != 0) {
        fprintf(stderr, "[WAREHOUSE] init FIFO '%s': %s\n", RESTOCK_FIFO, strerror(errno));
        return EXIT_FAILURE;
    }
    pthread_mutex_t orders_read_mutex;
    if (pthread_mutex_init(&orders_read_mutex, NULL) != 0) {
        fprintf(stderr, "[WAREHOUSE] orders_read mutex init failed.\n");
        return EXIT_FAILURE;
    }

    /* ---- shared order_id counter ---- */
    int next_order_id = 1;
    pthread_mutex_t order_id_mutex;
    if (pthread_mutex_init(&order_id_mutex, NULL) != 0) {
        fprintf(stderr, "[WAREHOUSE] order_id mutex init failed.\n");
        return EXIT_FAILURE;
    }

    /* ---- signals: block now (single-thread) so threads inherit
     * the mask and never receive them; main waits via sigsuspend. ---- */
    sigset_t block_set, empty_set;
    sigemptyset(&block_set);
    sigaddset(&block_set, SIGTERM);
    sigaddset(&block_set, SIGINT);
    sigaddset(&block_set, SIGUSR1);
    sigprocmask(SIG_BLOCK, &block_set, NULL);
    sigemptyset(&empty_set);               /* empty mask for sigsuspend */

    setup_handler(SIGTERM, handle_shutdown);
    setup_handler(SIGINT,  handle_shutdown);
    setup_handler(SIGUSR1, handle_status_dump);
    setup_handler(SIGPIPE, SIG_IGN);       /* dead client -> EPIPE, not death */

    /* ---- thread-argument structs ---- */
    ReceiverArgs receiver_args = { orders_fd, &orders_read_mutex, &inv, &pending,
                        &next_order_id, &order_id_mutex, log_fd, &log_mutex };
    PickerArgs   picker_args = { &inv, &pending, &packaging, log_fd, &log_mutex };
    PackerArgs   packer_args = { &packaging, log_fd, &log_mutex };
    RestockArgs  restock_args = { restock_fd, &inv };

    /* ---- spawn the thread pools ---- */
    pthread_t *recv_th = malloc((size_t)num_receivers * sizeof(pthread_t));
    pthread_t *pick_th = malloc((size_t)num_pickers   * sizeof(pthread_t));
    pthread_t *pack_th = malloc((size_t)num_packers   * sizeof(pthread_t));
    pthread_t  restock_th;
    if (!recv_th || !pick_th || !pack_th) {
        fprintf(stderr, "[WAREHOUSE] thread's malloc failed.\n");
        return EXIT_FAILURE;
    }

    for (int i = 0; i < num_receivers; i++)
        if (pthread_create(&recv_th[i], NULL, receiver_thread, &receiver_args) != 0) {
            fprintf(stderr, "[WAREHOUSE] pthread_create (receiver) failed.\n");
            return EXIT_FAILURE;
        }
    for (int i = 0; i < num_pickers;   i++)
        if (pthread_create(&pick_th[i], NULL, picker_thread, &picker_args) != 0) {
            fprintf(stderr, "[WAREHOUSE] pthread_create (picker) failed.\n");
            return EXIT_FAILURE;
        }
    for (int i = 0; i < num_packers;   i++)
        if (pthread_create(&pack_th[i], NULL, packer_thread, &packer_args) != 0) {
            fprintf(stderr, "[WAREHOUSE] pthread_create (packer) failed.\n");
            return EXIT_FAILURE;
        }
    if (pthread_create(&restock_th, NULL, restock_thread, &restock_args) != 0) {
        fprintf(stderr, "[WAREHOUSE] pthread_create (restock) failed.\n");
        return EXIT_FAILURE;
    }

    /* ---- main loop: wait for signals ---- */
    while (!shutdown_flag) {
        sigsuspend(&empty_set);
        if (dump_status_flag) {
            write_status_file(&inv, &pending, &packaging,
                           num_receivers, num_pickers, num_packers);
            dump_status_flag = 0;
        }
    }

    /* ---- ordered shutdown  ----*/

    RestockMsg stop_msg = { RESTOCK_STOP_ID, 0, 0 };
    if (write_all(restock_dummy_fd, &stop_msg, sizeof(stop_msg)) < 0)
        perror("[WAREHOUSE] write sentinella restock");

    close(orders_dummy_fd);
    close(restock_dummy_fd);

    for (int i = 0; i < num_receivers; i++)
        if (pthread_join(recv_th[i], NULL) != 0)
            fprintf(stderr, "[WAREHOUSE] pthread_join (receiver) failed.\n");
    if (pthread_join(restock_th, NULL) != 0)
        fprintf(stderr, "[WAREHOUSE] pthread_join (restock) failed.\n");

    bq_shutdown(&pending);
    for (int i = 0; i < num_pickers;   i++)
        if (pthread_join(pick_th[i], NULL) != 0)
            fprintf(stderr, "[WAREHOUSE] pthread_join (picker) failed.\n");

    bq_shutdown(&packaging);
    for (int i = 0; i < num_packers;   i++)
        if (pthread_join(pack_th[i], NULL) != 0)
            fprintf(stderr, "[WAREHOUSE] pthread_join (packer) failed.\n");

    /* ---- cleanup resources ---- */
    close(orders_fd);
    close(restock_fd);
    close(log_fd);
    unlink(ORDERS_FIFO);
    unlink(RESTOCK_FIFO);
    unlink(STATUS_FILE);

    bq_destroy(&pending);
    bq_destroy(&packaging);
    pthread_mutex_destroy(&inv.mutex);
    pthread_mutex_destroy(&log_mutex);
    pthread_mutex_destroy(&orders_read_mutex);
    pthread_mutex_destroy(&order_id_mutex);

    free(recv_th);
    free(pick_th);
    free(pack_th);

    return EXIT_SUCCESS;
}
