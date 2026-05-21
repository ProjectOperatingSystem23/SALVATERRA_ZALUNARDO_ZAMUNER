#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <errno.h>
#include <stdint.h>

#include "common.h"

#define LOG_FILE      "orders.log"
#define PID_FILE      "/tmp/pids.txt"
#define STATUS_FILE   "/tmp/wh_status.tmp"

/*NOTA DELL AUTORE: I path delle FIFO sono definiti qui in common.h invece di usare getenv()
 * perché sono fissi e condivisi da tutti i processi (warehouse, supplier).
 * getenv() aggiungerebbe complessità senza benefici: richiederebbe export
 * nel bootstrap e causerebbe crash non ovvi se la variabile fosse assente. */

typedef struct {
    char client_id[MAX_CLIENT_ID];
    int  item_id;
    int  quantity;
    int  shipped_qty;
    int  status;
    char response_fifo[MAX_RESP_FIFO];
} Order;

static int num_receivers;
static int num_pickers;
static int num_packers;
static int queues_capacity;

static pthread_t *receiver_threads = NULL;
static pthread_t *picker_threads   = NULL;
static pthread_t *packer_threads   = NULL;
static pthread_t  restock_thread;

static void* receiver_thread_func(void *arg);
static void* picker_thread_func(void *arg);
static void* packer_thread_func(void *arg);
static void* restock_thread_func(void *arg);

int main(int argc, char *argv[])
{
    if (argc != 6) {
        fprintf(stderr,
                "Usage: %s <num_receivers> <num_pickers> <num_packers>"
                " <queues_capacity> <inventory.csv>\n",
                argv[0]);
        return EXIT_FAILURE;
    }
    num_receivers  = atoi(argv[1]);
    num_pickers    = atoi(argv[2]);
    num_packers    = atoi(argv[3]);
    queues_capacity = atoi(argv[4]);
    if (g_num_receivers <= 0 || g_num_pickers <= 0 ||
        g_num_packers   <= 0 || g_queue_capacity <= 0) {
        fprintf(stderr,
                "[warehouse] Invalid arguments: all counters must be greather than 0\n");
        return EXIT_FAILURE;
        }
    const char *inventory = argv[5];

}

/*
////////////////////////////////////////CLAUDATE////////////////////////////////////////////////////////////////////////
 */
 /* warehouse.c — Fulfillment Center Warehouse Process
 *
 * Usage:
 *   ./warehouse <num_receivers> <num_pickers> <num_packers>
 *               <queue_capacity> <inventory.csv>
 *
 * IPC layout:
 *   orders.fifo    — clients (order.sh) write OrderRequest structs here
 *   restock.fifo   — suppliers write RestockMessage structs here
 *   <resp_fifo>    — each client creates its own FIFO and sends the path
 *                    inside the OrderRequest; warehouse writes OrderResponse
 *
 * Signals:
 *   SIGTERM / SIGINT  → graceful shutdown
 *   SIGUSR1           → dump status to /tmp/wh_status.tmp (for manage.sh)
 */

#define _POSIX_C_SOURCE 200809L

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <signal.h>
#include <errno.h>
#include <time.h>

/* ═══════════════════════════════════════════════════════════════════════════
 * Error codes  (shared with Bash as numeric values) da mettere in common.h
 * ═══════════════════════════════════════════════════════════════════════════ */
#define ERR_OK              0
#define ERR_ITEM_NOT_FOUND  1
#define ERR_OUT_OF_STOCK    2
#define ERR_INVALID_QTY     3
#define ERR_QUEUE_FULL      4
#define ERR_IO              5
#define ERR_PARTIAL         6   /* partial fill: some units shipped, some rejected */

/* ═══════════════════════════════════════════════════════════════════════════
 * FIFO / file paths
 * ═══════════════════════════════════════════════════════════════════════════ */

#define ORDERS_FIFO   "orders.fifo"
#define RESTOCK_FIFO  "restock.fifo"
#define LOG_FILE      "orders.log"
#define PID_FILE      "/tmp/wh_pid.txt"
#define STATUS_FILE   "/tmp/wh_status.tmp"

/* ═══════════════════════════════════════════════════════════════════════════
 * Sizing limits
 * ═══════════════════════════════════════════════════════════════════════════ */
#define MAX_ITEMS      256
#define MAX_CLIENT_ID  64
#define MAX_DESC       128
#define MAX_CATEGORY   64
#define MAX_RESP_FIFO  256

/* ═══════════════════════════════════════════════════════════════════════════
 * Data structures
 * ═══════════════════════════════════════════════════════════════════════════ */

/* --- Item ----------------------------------------------------------------- */
typedef struct {
    int  item_id;
    char description[MAX_DESC];
    char category[MAX_CATEGORY];
    int  stock;
} Item;

/* --- Inventory ------------------------------------------------------------ */
typedef struct {
    Item            items[MAX_ITEMS];
    int             count;
    pthread_mutex_t mutex;          /* protects stock of every item */
} Inventory;

/* --- Order status --------------------------------------------------------- */
typedef enum {
    ORDER_RECEIVED,
    ORDER_PICKING,
    ORDER_PACKING,
    ORDER_SHIPPED,
    ORDER_REJECTED
} OrderStatus;

/* --- In-process order (passed through bounded queues) --------------------- */
typedef struct {
    char        client_id[MAX_CLIENT_ID];
    char        resp_fifo[MAX_RESP_FIFO];   /* path to client response FIFO */
    int         item_id;
    int         quantity;
    int         qty_shipped;    /* filled by picker */
    OrderStatus status;
    int         error_code;
} Order;

/* --- Wire format: what order.sh writes into orders.fifo ------------------ */
typedef struct {
    char client_id[MAX_CLIENT_ID];
    char resp_fifo[MAX_RESP_FIFO];
    int  item_id;
    int  quantity;
} OrderRequest;

/* --- Wire format: what warehouse writes back to client ------------------- */
typedef struct {
    int status;         /* ERR_* code */
    int qty_shipped;
    int qty_rejected;
} OrderResponse;

/* --- Wire format: what supplier writes into restock.fifo ----------------- */
typedef struct {
    int supplier_id;
    int item_id;
    int quantity;
} RestockMessage;

/* --- Bounded queue (circular buffer, mutex + 2 condvars) ----------------- */
typedef struct {
    Order          *buf;
    int             head, tail, size, capacity;
    pthread_mutex_t mutex;
    pthread_cond_t  not_full;
    pthread_cond_t  not_empty;
    int             shutdown;   /* set to 1 to wake all blocked threads */
} BoundedQueue;

/* ═══════════════════════════════════════════════════════════════════════════
 * Global state
 * ═══════════════════════════════════════════════════════════════════════════ */
static Inventory    g_inv;
static BoundedQueue g_pending;      /* Order Receivers  → Picker Robots  */
static BoundedQueue g_packaging;    /* Picker Robots    → Packers        */

static volatile sig_atomic_t g_shutdown = 0;

/* orders.fifo kept open (read + dummy-write) to avoid spurious EOF */
static int g_orders_fd       = -1;
static int g_orders_dummy_fd = -1;  /* write end held by warehouse itself */
static pthread_mutex_t g_orders_read_mutex = PTHREAD_MUTEX_INITIALIZER;

/* restock.fifo — same pattern */
static int g_restock_fd       = -1;
static int g_restock_dummy_fd = -1;

static FILE            *g_log        = NULL;
static pthread_mutex_t  g_log_mutex  = PTHREAD_MUTEX_INITIALIZER;

static int g_num_receivers, g_num_pickers, g_num_packers;

/* ═══════════════════════════════════════════════════════════════════════════
 * Utility
 * ═══════════════════════════════════════════════════════════════════════════ */

static void rand_sleep_1_3(void)
{
    sleep(1 + rand() % 3);  /* uniform in {1, 2, 3} seconds */
}

/* Safe read: retry on EINTR, return total bytes read or -1 on error */
static ssize_t read_all(int fd, void *buf, size_t len)
{
    size_t done = 0;
    while (done < len) {
        ssize_t n = read(fd, (char *)buf + done, len - done);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) break;  /* EOF */
        done += (size_t)n;
    }
    return (ssize_t)done;
}

/* Safe write: retry on EINTR */
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
 * Inventory
 * ═══════════════════════════════════════════════════════════════════════════ */

/*
 * Parse one CSV field that may be quoted ("foo,bar") or plain (foo).
 * Advances *pp past the field and the following comma (if any).
 * Writes at most (dst_size-1) bytes into dst and NUL-terminates.
 */
static void csv_field(char **pp, char *dst, int dst_size)
{
    char *p = *pp;
    int   i = 0;

    if (*p == '"') {
        p++;                            /* skip opening quote */
        while (*p && *p != '"') {
            if (i < dst_size - 1) dst[i++] = *p;
            p++;
        }
        if (*p == '"') p++;            /* skip closing quote */
    } else {
        while (*p && *p != ',' && *p != '\n' && *p != '\r') {
            if (i < dst_size - 1) dst[i++] = *p;
            p++;
        }
    }
    dst[i] = '\0';
    if (*p == ',') p++;                /* skip separator */
    *pp = p;
}

static int inventory_load(Inventory *inv, const char *path)
{
    FILE *f = fopen(path, "r");
    if (!f) {
        fprintf(stderr, "[WAREHOUSE] Cannot open inventory file '%s': %s\n",
                path, strerror(errno));
        return ERR_IO;
    }

    char line[512];
    /* skip header row */
    if (!fgets(line, sizeof(line), f)) {
        fclose(f);
        fprintf(stderr, "[WAREHOUSE] Inventory file is empty.\n");
        return ERR_IO;
    }

    inv->count = 0;
    while (fgets(line, sizeof(line), f)) {
        if (line[0] == '\n' || line[0] == '\r' || line[0] == '\0') continue;
        if (inv->count >= MAX_ITEMS) {
            fprintf(stderr, "[WAREHOUSE] Warning: inventory exceeds MAX_ITEMS=%d, truncating.\n",
                    MAX_ITEMS);
            break;
        }

        Item *it = &inv->items[inv->count];
        char *p  = line;

        /* item_id */
        char tmp[32];
        csv_field(&p, tmp, sizeof(tmp));
        it->item_id = atoi(tmp);
        if (it->item_id <= 0) continue;    /* skip malformed row */

        /* description */
        csv_field(&p, it->description, sizeof(it->description));

        /* category */
        csv_field(&p, it->category, sizeof(it->category));

        /* stock */
        csv_field(&p, tmp, sizeof(tmp));
        it->stock = atoi(tmp);

        inv->count++;
    }

    fclose(f);
    return ERR_OK;
}

/* Returns pointer to item with given id, or NULL.  Caller must hold inv->mutex. */
static Item *inventory_find_locked(Inventory *inv, int item_id)
{
    for (int i = 0; i < inv->count; i++)
        if (inv->items[i].item_id == item_id)
            return &inv->items[i];
    return NULL;
}

/*
 * Atomically decrement stock.
 * Returns the number of units actually shipped (0 … qty).
 * Sets *err to ERR_OK, ERR_PARTIAL, ERR_OUT_OF_STOCK, or ERR_ITEM_NOT_FOUND.
 * Caller must hold inv->mutex.
 */
static int inventory_decrement_locked(Inventory *inv, int item_id,
                                      int qty, int *err)
{
    Item *it = inventory_find_locked(inv, item_id);
    if (!it)           { *err = ERR_ITEM_NOT_FOUND; return 0; }
    if (it->stock <= 0){ *err = ERR_OUT_OF_STOCK;   return 0; }

    int ship = (it->stock >= qty) ? qty : it->stock;
    it->stock -= ship;
    *err = (ship < qty) ? ERR_PARTIAL : ERR_OK;
    return ship;
}

/* Increment stock (called from restock thread). */
static void inventory_increment(Inventory *inv, int item_id, int qty)
{
    pthread_mutex_lock(&inv->mutex);
    Item *it = inventory_find_locked(inv, item_id);
    if (it) {
        it->stock += qty;
        fprintf(stdout, "[RESTOCK] item_id=%d  +%d units  (new stock: %d)\n",
                item_id, qty, it->stock);
    } else {
        fprintf(stderr, "[RESTOCK] Warning: item_id=%d not found in inventory.\n",
                item_id);
    }
    pthread_mutex_unlock(&inv->mutex);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Bounded Queue
 * ═══════════════════════════════════════════════════════════════════════════ */

static int bq_init(BoundedQueue *q, int capacity)
{
    q->buf = calloc((size_t)capacity, sizeof(Order));
    if (!q->buf) return ERR_IO;

    q->head = q->tail = q->size = 0;
    q->capacity = capacity;
    q->shutdown  = 0;

    if (pthread_mutex_init(&q->mutex,    NULL) != 0) goto fail;
    if (pthread_cond_init (&q->not_full, NULL) != 0) goto fail;
    if (pthread_cond_init (&q->not_empty,NULL) != 0) goto fail;
    return ERR_OK;

fail:
    free(q->buf);
    return ERR_IO;
}

static void bq_destroy(BoundedQueue *q)
{
    pthread_mutex_destroy(&q->mutex);
    pthread_cond_destroy (&q->not_full);
    pthread_cond_destroy (&q->not_empty);
    free(q->buf);
}

/*
 * Push an order.  Blocks if the queue is full (producer-side).
 * Returns 0 on success, -1 if the queue is being shut down.
 */
static int bq_push(BoundedQueue *q, const Order *order)
{
    pthread_mutex_lock(&q->mutex);

    while (q->size == q->capacity && !q->shutdown)
        pthread_cond_wait(&q->not_full, &q->mutex);

    if (q->shutdown) {
        pthread_mutex_unlock(&q->mutex);
        return -1;
    }

    q->buf[q->tail] = *order;
    q->tail = (q->tail + 1) % q->capacity;
    q->size++;

    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->mutex);
    return 0;
}

/*
 * Pop an order.  Blocks if the queue is empty (consumer-side).
 * Returns 0 on success, -1 if shut down and nothing left.
 */
static int bq_pop(BoundedQueue *q, Order *out)
{
    pthread_mutex_lock(&q->mutex);

    while (q->size == 0 && !q->shutdown)
        pthread_cond_wait(&q->not_empty, &q->mutex);

    if (q->size == 0) {             /* shutdown with empty queue */
        pthread_mutex_unlock(&q->mutex);
        return -1;
    }

    *out    = q->buf[q->head];
    q->head = (q->head + 1) % q->capacity;
    q->size--;

    pthread_cond_signal(&q->not_full);
    pthread_mutex_unlock(&q->mutex);
    return 0;
}

/* Wake all blocked threads so they can exit. */
static void bq_shutdown(BoundedQueue *q)
{
    pthread_mutex_lock(&q->mutex);
    q->shutdown = 1;
    pthread_cond_broadcast(&q->not_full);
    pthread_cond_broadcast(&q->not_empty);
    pthread_mutex_unlock(&q->mutex);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Logging
 * ═══════════════════════════════════════════════════════════════════════════ */

static void log_order(const Order *o)
{
    char tbuf[32];
    time_t now = time(NULL);
    struct tm *tm_info = localtime(&now);
    strftime(tbuf, sizeof(tbuf), "%Y-%m-%dT%H:%M:%S", tm_info);

    const char *s = "UNKNOWN";
    if      (o->status == ORDER_SHIPPED)  s = "SHIPPED";
    else if (o->status == ORDER_REJECTED) s = "REJECTED";
    else if (o->error_code == ERR_PARTIAL)s = "PARTIAL";

    pthread_mutex_lock(&g_log_mutex);
    if (g_log) {
        /* timestamp|client_id|item_id|qty_requested|qty_shipped|status|err */
        fprintf(g_log, "%s|%s|%d|%d|%d|%s|%d\n",
                tbuf,
                o->client_id,
                o->item_id,
                o->quantity,
                o->qty_shipped,
                s,
                o->error_code);
        fflush(g_log);
    }
    pthread_mutex_unlock(&g_log_mutex);
}

/* Send an OrderResponse to the client's private FIFO (non-blocking). */
static void send_response(const char *resp_fifo, int status,
                          int qty_shipped, int qty_rejected)
{
    if (resp_fifo[0] == '\0') return;

    OrderResponse resp = { status, qty_shipped, qty_rejected };
    int fd = open(resp_fifo, O_WRONLY | O_NONBLOCK);
    if (fd < 0) {
        /* Client may have timed out — not a fatal error */
        fprintf(stderr, "[WAREHOUSE] Warning: cannot open resp_fifo '%s': %s\n",
                resp_fifo, strerror(errno));
        return;
    }
    write_all(fd, &resp, sizeof(resp));
    close(fd);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Thread: Order Receiver
 *
 * Multiple receiver threads all read from the same g_orders_fd.
 * A mutex ensures each thread gets exactly one complete OrderRequest.
 * ═══════════════════════════════════════════════════════════════════════════ */
static void *thread_receiver(void *arg)
{
    (void)arg;

    while (!g_shutdown) {
        OrderRequest req;

        /* Mutex-protected read so each receiver gets exactly one message */
        pthread_mutex_lock(&g_orders_read_mutex);
        ssize_t n = read_all(g_orders_fd, &req, sizeof(req));
        pthread_mutex_unlock(&g_orders_read_mutex);

        if (g_shutdown) break;

        if (n != (ssize_t)sizeof(req)) {
            if (n < 0 && errno != EINTR)
                perror("[RECEIVER] read");
            continue;
        }

        /* ── Validate ───────────────────────────────────────────────────── */
        if (req.quantity <= 0) {
            fprintf(stderr, "[RECEIVER] client=%s invalid quantity=%d\n",
                    req.client_id, req.quantity);
            send_response(req.resp_fifo, ERR_INVALID_QTY, 0, req.quantity);
            continue;
        }

        /* Check item existence and stock (read-only peek, lock still needed) */
        pthread_mutex_lock(&g_inv.mutex);
        Item *it       = inventory_find_locked(&g_inv, req.item_id);
        int   exists   = (it != NULL);
        int   in_stock = exists && (it->stock > 0);
        pthread_mutex_unlock(&g_inv.mutex);

        if (!exists) {
            fprintf(stderr, "[RECEIVER] client=%s item_id=%d NOT FOUND\n",
                    req.client_id, req.item_id);
            send_response(req.resp_fifo, ERR_ITEM_NOT_FOUND, 0, req.quantity);
            continue;
        }
        if (!in_stock) {
            fprintf(stderr, "[RECEIVER] client=%s item_id=%d OUT OF STOCK\n",
                    req.client_id, req.item_id);
            send_response(req.resp_fifo, ERR_OUT_OF_STOCK, 0, req.quantity);
            continue;
        }

        /* ── Enqueue ────────────────────────────────────────────────────── */
        Order order;
        memset(&order, 0, sizeof(order));
        strncpy(order.client_id, req.client_id, MAX_CLIENT_ID - 1);
        strncpy(order.resp_fifo, req.resp_fifo,  MAX_RESP_FIFO  - 1);
        order.item_id  = req.item_id;
        order.quantity = req.quantity;
        order.status   = ORDER_RECEIVED;

        fprintf(stdout, "[RECEIVER] Queuing order: client=%s item=%d qty=%d\n",
                order.client_id, order.item_id, order.quantity);

        if (bq_push(&g_pending, &order) != 0)
            break;      /* queue was shut down */
    }

    return NULL;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Thread: Picker Robot
 * ═══════════════════════════════════════════════════════════════════════════ */
static void *thread_picker(void *arg)
{
    (void)arg;

    while (1) {
        Order order;
        if (bq_pop(&g_pending, &order) != 0) break;

        order.status = ORDER_PICKING;
        fprintf(stdout, "[PICKER] Picking: client=%s item=%d qty=%d\n",
                order.client_id, order.item_id, order.quantity);

        rand_sleep_1_3();   /* simulate physical picking time */

        /* ── Decrement inventory ────────────────────────────────────────── */
        int err;
        pthread_mutex_lock(&g_inv.mutex);
        int shipped = inventory_decrement_locked(&g_inv, order.item_id,
                                                 order.quantity, &err);
        pthread_mutex_unlock(&g_inv.mutex);

        order.qty_shipped = shipped;
        order.error_code  = err;

        if (err == ERR_ITEM_NOT_FOUND || err == ERR_OUT_OF_STOCK) {
            /* Item disappeared between validation and picking — reject */
            order.status = ORDER_REJECTED;
            fprintf(stderr, "[PICKER] Rejecting (err=%d): client=%s item=%d\n",
                    err, order.client_id, order.item_id);
            send_response(order.resp_fifo, err, 0, order.quantity);
            log_order(&order);
            continue;
        }

        /* ── Forward to packaging ───────────────────────────────────────── */
        order.status = ORDER_PACKING;
        fprintf(stdout, "[PICKER] Done picking: client=%s item=%d shipped=%d\n",
                order.client_id, order.item_id, shipped);

        if (bq_push(&g_packaging, &order) != 0) break;
    }

    return NULL;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Thread: Packer
 * ═══════════════════════════════════════════════════════════════════════════ */
static void *thread_packer(void *arg)
{
    (void)arg;

    while (1) {
        Order order;
        if (bq_pop(&g_packaging, &order) != 0) break;

        fprintf(stdout, "[PACKER] Packing: client=%s item=%d qty_shipped=%d\n",
                order.client_id, order.item_id, order.qty_shipped);

        rand_sleep_1_3();   /* simulate packing time */

        order.status = ORDER_SHIPPED;
        int rejected = order.quantity - order.qty_shipped;

        /* Determine effective status code for client */
        int client_status = (order.error_code == ERR_PARTIAL)
                            ? ERR_PARTIAL
                            : ERR_OK;

        send_response(order.resp_fifo, client_status,
                      order.qty_shipped, rejected);
        log_order(&order);

        fprintf(stdout, "[PACKER] Shipped: client=%s item=%d shipped=%d rejected=%d\n",
                order.client_id, order.item_id, order.qty_shipped, rejected);
    }

    return NULL;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Thread: Restock Listener
 *
 * Reads RestockMessage structs from restock.fifo and increments inventory.
 * ═══════════════════════════════════════════════════════════════════════════ */
static void *thread_restock(void *arg)
{
    (void)arg;

    while (!g_shutdown) {
        RestockMessage msg;
        ssize_t n = read_all(g_restock_fd, &msg, sizeof(msg));

        if (g_shutdown) break;

        if (n != (ssize_t)sizeof(msg)) {
            if (n < 0 && errno != EINTR)
                perror("[RESTOCK] read");
            continue;
        }

        inventory_increment(&g_inv, msg.item_id, msg.quantity);
    }

    return NULL;
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Signal handlers
 * ═══════════════════════════════════════════════════════════════════════════ */

/*
 * SIGTERM / SIGINT — graceful shutdown.
 *
 * 1. Set the shutdown flag.
 * 2. Wake all threads blocked on the queues.
 * 3. Unblock receiver/restock threads stuck in read() by closing the
 *    dummy write ends, which causes read() to return 0 (EOF).
 */
static void handle_sigterm(int sig)
{
    (void)sig;
    g_shutdown = 1;

    bq_shutdown(&g_pending);
    bq_shutdown(&g_packaging);

    /* Closing the write end of each FIFO causes the read end to get EOF */
    if (g_orders_dummy_fd  >= 0) { close(g_orders_dummy_fd);  g_orders_dummy_fd  = -1; }
    if (g_restock_dummy_fd >= 0) { close(g_restock_dummy_fd); g_restock_dummy_fd = -1; }
}

/*
 * SIGUSR1 — status dump for manage.sh.
 *
 * Writes a snapshot of queue sizes and inventory to STATUS_FILE.
 * Signal-handler-safe: uses only async-signal-safe calls (open/write/close)
 * except for the queue size reads (acceptable for a status snapshot).
 */
static void handle_sigusr1(int sig)
{
    (void)sig;

    FILE *f = fopen(STATUS_FILE, "w");
    if (!f) return;

    fprintf(f, "PID=%d\n",             (int)getpid());
    fprintf(f, "RECEIVERS=%d\n",       g_num_receivers);
    fprintf(f, "PICKERS=%d\n",         g_num_pickers);
    fprintf(f, "PACKERS=%d\n",         g_num_packers);
    fprintf(f, "PENDING_QUEUE=%d/%d\n",   g_pending.size,   g_pending.capacity);
    fprintf(f, "PACKAGING_QUEUE=%d/%d\n", g_packaging.size, g_packaging.capacity);
    fprintf(f, "INVENTORY_COUNT=%d\n", g_inv.count);

    for (int i = 0; i < g_inv.count; i++) {
        Item *it = &g_inv.items[i];
        /* ITEM|id|description|category|stock */
        fprintf(f, "ITEM|%d|%s|%s|%d\n",
                it->item_id, it->description, it->category, it->stock);
    }

    fclose(f);
}

/* ═══════════════════════════════════════════════════════════════════════════
 * Main
 * ═══════════════════════════════════════════════════════════════════════════ */
int main(int argc, char *argv[])
{
    if (argc != 6) {
        fprintf(stderr,
            "Usage: %s <num_receivers> <num_pickers> <num_packers>"
            " <queue_capacity> <inventory.csv>\n", argv[0]);
        return 1;
    }

    g_num_receivers   = atoi(argv[1]);
    g_num_pickers     = atoi(argv[2]);
    g_num_packers     = atoi(argv[3]);
    int  queue_cap    = atoi(argv[4]);
    const char *inv_path = argv[5];

    if (g_num_receivers <= 0 || g_num_pickers <= 0 ||
        g_num_packers   <= 0 || queue_cap     <= 0) {
        fprintf(stderr, "[WAREHOUSE] All numeric arguments must be > 0.\n");
        return 1;
    }

    srand((unsigned)time(NULL) ^ (unsigned)getpid());

    /* ── Load inventory ──────────────────────────────────────────────────── */
    pthread_mutex_init(&g_inv.mutex, NULL);
    if (inventory_load(&g_inv, inv_path) != ERR_OK) {
        fprintf(stderr, "[WAREHOUSE] Failed to load inventory from '%s'.\n", inv_path);
        return 1;
    }
    fprintf(stdout, "[WAREHOUSE] Loaded %d items from '%s'.\n",
            g_inv.count, inv_path);

    /* ── Open log file ───────────────────────────────────────────────────── */
    g_log = fopen(LOG_FILE, "a");
    if (!g_log) {
        fprintf(stderr, "[WAREHOUSE] Cannot open log file '%s': %s\n",
                LOG_FILE, strerror(errno));
        return 1;
    }

    /* ── Init bounded queues ─────────────────────────────────────────────── */
    if (bq_init(&g_pending,   queue_cap) != ERR_OK ||
        bq_init(&g_packaging, queue_cap) != ERR_OK) {
        fprintf(stderr, "[WAREHOUSE] Failed to initialise bounded queues.\n");
        return 1;
    }

    /* ── Open FIFOs ──────────────────────────────────────────────────────── */
    /*
     * Opening strategy: open for reading in non-blocking mode first,
     * then open a dummy write-end (so read() never returns EOF when
     * no client is connected), then switch back to blocking mode.
     */

    /* orders.fifo */
    if (mkfifo(ORDERS_FIFO, 0666) != 0 && errno != EEXIST) {
        perror("[WAREHOUSE] mkfifo orders.fifo");
        return 1;
    }
    g_orders_fd = open(ORDERS_FIFO, O_RDONLY | O_NONBLOCK);
    if (g_orders_fd < 0) { perror("[WAREHOUSE] open orders.fifo (read)"); return 1; }
    g_orders_dummy_fd = open(ORDERS_FIFO, O_WRONLY | O_NONBLOCK);
    if (g_orders_dummy_fd < 0) { perror("[WAREHOUSE] open orders.fifo (write)"); return 1; }
    /* switch to blocking mode for the read end */
    fcntl(g_orders_fd, F_SETFL,
          fcntl(g_orders_fd, F_GETFL, 0) & ~O_NONBLOCK);

    /* restock.fifo */
    if (mkfifo(RESTOCK_FIFO, 0666) != 0 && errno != EEXIST) {
        perror("[WAREHOUSE] mkfifo restock.fifo");
        return 1;
    }
    g_restock_fd = open(RESTOCK_FIFO, O_RDONLY | O_NONBLOCK);
    if (g_restock_fd < 0) { perror("[WAREHOUSE] open restock.fifo (read)"); return 1; }
    g_restock_dummy_fd = open(RESTOCK_FIFO, O_WRONLY | O_NONBLOCK);
    if (g_restock_dummy_fd < 0) { perror("[WAREHOUSE] open restock.fifo (write)"); return 1; }
    fcntl(g_restock_fd, F_SETFL,
          fcntl(g_restock_fd, F_GETFL, 0) & ~O_NONBLOCK);

    /* ── Save PID ────────────────────────────────────────────────────────── */
    {
        FILE *pf = fopen(PID_FILE, "w");
        if (pf) { fprintf(pf, "%d\n", (int)getpid()); fclose(pf); }
    }

    /* ── Install signal handlers ─────────────────────────────────────────── */
    {
        struct sigaction sa;
        memset(&sa, 0, sizeof(sa));
        sigemptyset(&sa.sa_mask);

        sa.sa_handler = handle_sigterm;
        sigaction(SIGTERM, &sa, NULL);
        sigaction(SIGINT,  &sa, NULL);

        sa.sa_handler = handle_sigusr1;
        sigaction(SIGUSR1, &sa, NULL);

        /* Ignore SIGPIPE (broken pipe when a client response FIFO is gone) */
        sa.sa_handler = SIG_IGN;
        sigaction(SIGPIPE, &sa, NULL);
    }

    /* ── Spawn thread pools ──────────────────────────────────────────────── */
    pthread_t *recv_th = calloc((size_t)g_num_receivers, sizeof(pthread_t));
    pthread_t *pick_th = calloc((size_t)g_num_pickers,   sizeof(pthread_t));
    pthread_t *pack_th = calloc((size_t)g_num_packers,   sizeof(pthread_t));
    pthread_t  rest_th;

    if (!recv_th || !pick_th || !pack_th) {
        fprintf(stderr, "[WAREHOUSE] calloc failed.\n");
        return 1;
    }

    for (int i = 0; i < g_num_receivers; i++)
        pthread_create(&recv_th[i], NULL, thread_receiver, NULL);
    for (int i = 0; i < g_num_pickers; i++)
        pthread_create(&pick_th[i], NULL, thread_picker, NULL);
    for (int i = 0; i < g_num_packers; i++)
        pthread_create(&pack_th[i], NULL, thread_packer, NULL);
    pthread_create(&rest_th, NULL, thread_restock, NULL);

    fprintf(stdout,
        "[WAREHOUSE] Running — receivers=%d pickers=%d packers=%d"
        " queue_cap=%d  PID=%d\n",
        g_num_receivers, g_num_pickers, g_num_packers,
        queue_cap, (int)getpid());

    /* ── Wait for all threads ────────────────────────────────────────────── */
    for (int i = 0; i < g_num_receivers; i++) pthread_join(recv_th[i], NULL);
    for (int i = 0; i < g_num_pickers;   i++) pthread_join(pick_th[i], NULL);
    for (int i = 0; i < g_num_packers;   i++) pthread_join(pack_th[i], NULL);
    pthread_join(rest_th, NULL);

    /* ── Cleanup ─────────────────────────────────────────────────────────── */
    if (g_orders_fd  >= 0) close(g_orders_fd);
    if (g_restock_fd >= 0) close(g_restock_fd);

    bq_destroy(&g_pending);
    bq_destroy(&g_packaging);
    pthread_mutex_destroy(&g_inv.mutex);
    pthread_mutex_destroy(&g_log_mutex);
    pthread_mutex_destroy(&g_orders_read_mutex);

    if (g_log) fclose(g_log);
    free(recv_th);
    free(pick_th);
    free(pack_th);

    unlink(PID_FILE);

    fprintf(stdout, "[WAREHOUSE] Shutdown complete.\n");
    return 0;
}