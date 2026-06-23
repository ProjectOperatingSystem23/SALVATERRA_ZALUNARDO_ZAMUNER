/* ============================================================================
 * order_helper.c: C helper of order.sh.
 *
 * Usage (invoked by order.sh):
 *   ./order_helper <client_id> <item_id> <quantity>
 *
 * Client side of the request/response FIFO protocol.
 * Flow: create a private response FIFO /tmp/order_resp_<PID>, open its read-end
 * BEFORE sending, write an OrderRequest on ORDERS_FIFO, wait for the
 * OrderResponse, print it and return the status as the exit code.
 * ============================================================================ */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <errno.h>
#include "common.h"

/* Max wait (s) for the warehouse reply: covers queueing + pick/pack sleeps. */
#define RESP_TIMEOUT_SECONDS  30

static volatile sig_atomic_t timed_out_flag = 0;   /* set by SIGALRM */

static void on_alarm(int sig) { (void)sig; timed_out_flag = 1; }

/* Timeout-aware full read of the response: on EINTR, give up if the alarm fired, otherwise retry.
 * Returns bytes read, 0 = EOF, -1 = error. */
static ssize_t read_response(int fd, void *buf, size_t len)
{
    size_t done = 0;
    while (done < len) {
        ssize_t n = read(fd, (char *)buf + done, len - done);
        if (n < 0) {
            if (errno == EINTR) {
                if (timed_out_flag) return -1;
                continue;
            }
            return -1;
        }
        if (n == 0) break;
        done += (size_t)n;
    }
    return (ssize_t)done;
}

/* Render the binary OrderResponse as human-readable output for the user. */
static void print_outcome(const char *client_id, const OrderResponse *r)
{
    switch (r->status) {
    case ERR_OK:
        printf("[OK] %s: orders complete item=%d  shipped=%d/%d\n", client_id, r->item_id, r->qty_shipped, r->qty_requested);
        break;
    case ERR_PARTIAL_FILL:
        printf("[PARTIAL] %s: item=%d  shipped=%d/%d  (rejected=%d, insufficient stock)\n", client_id, r->item_id, r->qty_shipped, r->qty_requested, r->qty_rejected);
        break;
    case ERR_ITEM_NOT_FOUND:
        printf("[REJECTED] %s: item=%d does not exist in the inventory\n", client_id, r->item_id);
        break;
    case ERR_OUT_OF_STOCK:
        printf("[REJECTED] %s: item=%d is out of stock\n", client_id, r->item_id);
        break;
    case ERR_INVALID_QTY:
        printf("[REJECTED] %s: invalid quantity (%d)\n", client_id, r->qty_requested);
        break;
    default:
        printf("[REJECTED] %s: item=%d  status=%d\n", client_id, r->item_id, r->status);
        break;
    }
}

/* ====== MAIN ====== */
int main(int argc, char *argv[])
{
    if (argc != 4) {
        fprintf(stderr, "Usage: %s <client_id> <item_id> <quantity>\n", argv[0]);
        return ERR_USAGE;
    }
    const char *client_id = argv[1];
    int item_id  = atoi(argv[2]);   /* order.sh already validated these */
    int quantity = atoi(argv[3]);   /* values <= 0 are validated by the warehouse */

    /* client_id must be non-empty and fit the struct field. */
    if (client_id[0] == '\0' || strlen(client_id) >= MAX_CLIENT_ID) {
        fprintf(stderr, "[ORDER] client_id missing or too long (max %d)\n",                MAX_CLIENT_ID - 1);
        return ERR_USAGE;
    }

    /* timeout + survive a warehouse that dies. */
    setup_handler(SIGALRM, on_alarm);
    setup_handler(SIGPIPE, SIG_IGN);

    /* ---- private response FIFO: /tmp/order_resp_<PID> ---- */
    char resp_path[MAX_RESP_FIFO];
    snprintf(resp_path, sizeof(resp_path), RESP_FIFO_TEMPLATE, (int)getpid());

    int resp_r_fd, resp_dummy_w_fd;
    if (open_fifo_r_dw(resp_path, 0600, &resp_r_fd, &resp_dummy_w_fd) != 0) {
        fprintf(stderr, "[ORDER] init resp_fifo '%s': %s\n", resp_path, strerror(errno));
        unlink(resp_path);
        return ERR_IO;
    }

    /* Arm the global timeout. */
    alarm(RESP_TIMEOUT_SECONDS);

    /* ---- open ORDERS_FIFO for writing (blocking) ----*/
    int ofd = -1;
    while (ofd < 0) {
        ofd = open(ORDERS_FIFO, O_WRONLY);
        if (ofd < 0) {
            if (errno == EINTR) { /* EINTR: exit on timeout, otherwise retry. */
                if (timed_out_flag) {
                    fprintf(stderr, "[ORDER] timeout while opening ORDERS_FIFO (warehouse is not responding)\n");
                    close(resp_r_fd);
                    close(resp_dummy_w_fd);
                    unlink(resp_path);
                    return ERR_TIMEOUT;
                }
                continue;
            }
            fprintf(stderr, "[ORDER] open '%s': %s (is the warehouse active?)\n", ORDERS_FIFO, strerror(errno));
            close(resp_r_fd);
            close(resp_dummy_w_fd);
            unlink(resp_path);
            return ERR_WAREHOUSE_DOWN;
        }
    }

    /* ---- build and send the OrderRequest ---- */
    OrderRequest req;
    memset(&req, 0, sizeof(req));
    snprintf(req.client_id, sizeof(req.client_id), "%s", client_id);
    snprintf(req.resp_fifo, sizeof(req.resp_fifo), "%s", resp_path);
    req.item_id  = item_id;
    req.quantity = quantity;
    if (write_all(ofd, &req, sizeof(req)) < 0) {
        fprintf(stderr, "[ORDER] write to ORDERS_FIFO: %s\n", strerror(errno));
        close(ofd);
        close(resp_r_fd);
        close(resp_dummy_w_fd);
        unlink(resp_path);
        return ERR_IO;
    }
    close(ofd);

    /* ----  reads the response on the private FIFO ---- */
    OrderResponse resp;
    memset(&resp, 0, sizeof(resp));
    ssize_t n = read_response(resp_r_fd, &resp, sizeof(resp));
    alarm(0); /*disarm the timeout*/

    close(resp_r_fd);
    close(resp_dummy_w_fd);
    unlink(resp_path);

    if (n != (ssize_t)sizeof(resp)) {
        if (timed_out_flag) {
            fprintf(stderr, "[ORDER] timeout: no response from the warehouse within %d s\n", RESP_TIMEOUT_SECONDS);
            return ERR_TIMEOUT;
        }
        fprintf(stderr, "[ORDER] missing or truncated response (%zd byte)\n", n);
        return ERR_IO;
    }

    print_outcome(client_id, &resp);
    return resp.status;
}