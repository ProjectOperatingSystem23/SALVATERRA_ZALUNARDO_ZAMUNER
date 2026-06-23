/* ============================================================================
 * manage_restock_helper.c: C helper of manage.sh.
 *
 * Usage (invoked by manage.sh restock):
 *   ./manage_restock_helper <item_id> <quantity>
 *
 * Manager side of the restock channel.
 * Validates item_id/quantity (>= 1), opens RESTOCK_FIFO non-blocking and writes
 * one RestockMsg with supplier_id = MANUAL_RESTOCK_SUPPLIER_ID (0).
 * ============================================================================ */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <errno.h>
#include <string.h>
#include "common.h"

int main(int argc, char *argv[])
{
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <item_id> <quantity>\n", argv[0]);
        return ERR_USAGE;
    }

    int item_id  = atoi(argv[1]);
    int quantity = atoi(argv[2]);

    if (item_id <= 0) {
        fprintf(stderr, "[RESTOCK] item_id must be >= 1 (received '%s')\n", argv[1]);
        return ERR_USAGE;
    }
    if (quantity <= 0) {
        fprintf(stderr, "[RESTOCK] quantity must be >= 1 (received '%s')\n", argv[2]);
        return ERR_USAGE;
    }

    /* SIGPIPE ignored */
    setup_handler(SIGPIPE, SIG_IGN);

    /* Non-blocking open: fails if the warehouse (the reader) is absent. */
    int fd = open(RESTOCK_FIFO, O_WRONLY | O_NONBLOCK);
    if (fd < 0) {
        if (errno == ENXIO || errno == ENOENT) {
            fprintf(stderr, "[RESTOCK] warehouse down (FIFO '%s': %s)\n", RESTOCK_FIFO, strerror(errno));
            return ERR_WAREHOUSE_DOWN;
        }
        fprintf(stderr, "[RESTOCK] failed to open '%s': %s\n", RESTOCK_FIFO, strerror(errno));
        return ERR_IO;
    }

    /* supplier_id = 0 -> MANUAL restock. */
    RestockMsg msg = { MANUAL_RESTOCK_SUPPLIER_ID, item_id, quantity };
    if (write_all(fd, &msg, sizeof(msg)) < 0) {
        fprintf(stderr, "[RESTOCK] failed to write on '%s': %s\n", RESTOCK_FIFO, strerror(errno));
        close(fd);
        return ERR_IO;
    }

    close(fd);
    return ERR_OK;
}