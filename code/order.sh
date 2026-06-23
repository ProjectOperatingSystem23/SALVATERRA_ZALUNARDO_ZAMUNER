#!/bin/bash
# =============================================================================
# order.sh -- Send an order to the Fulfillment Center.
#
# Usage:
#   ./order.sh <client_id> <item_id> <quantity>
#
# Validates the arguments, checks the warehouse is alive,
# then delegates the binary IPC to ./order_helper and re-propagates its exit
# code.
# =============================================================================

# ---- error codes ----
ERR_IO=4
ERR_WAREHOUSE_DOWN=6
ERR_USAGE=8

# ---- paths ----
ORDERS_FIFO="/tmp/orders_fifo"
WAREHOUSE_PID_FILE="/tmp/warehouse.pid"

# ---- C helper (relative path: run from the project directory) ----
HELPER="./order_helper"

# err: message to stderr (fd 2).
err() { printf '%s\n' "$*" >&2; }

# die <exit_code> <message...>: print the error and exit with the ERR_* code.
die() {
    local code=$1
    shift
    err "$*"
    exit "$code"
}

# ---- usage ----
if [ "$#" -ne 3 ]; then
    die "$ERR_USAGE" "Use: $0 <client_id> <item_id> <quantity>"
fi

CLIENT_ID=$1
ITEM_ID=$2
QUANTITY=$3

# ----  arguments validation ----
case "$CLIENT_ID" in
    "")
        die "$ERR_USAGE" "Error: empty client_id." ;;
    *[!A-Za-z0-9_.-]*)
        die "$ERR_USAGE" "Error: client_id contains invalid characters (use A-Z, a-z, 0-9, _, ., -)." ;;
esac
if [ "${#CLIENT_ID}" -ge 64 ]; then
    die "$ERR_USAGE" "Error: client_id is too long (max 63 characters)."
fi

case "$ITEM_ID" in
    ''|*[!0-9]*) die "$ERR_USAGE" "Error: item_id ('$ITEM_ID') is not a positive integer." ;;
    *[1-9]*)     : ;;                                                            # > 0 -> ok
    *)           die "$ERR_USAGE" "Error: item_id must be >= 1." ;;
esac

case "$QUANTITY" in
    ''|*[!0-9]*) die "$ERR_USAGE" "Error: quantity ('$QUANTITY') is not a positive integer." ;;
    *[1-9]*)     : ;;                                                             # > 0 -> ok
    *)           die "$ERR_USAGE" "Error: quantity must be >= 1." ;;
esac

if [ ! -f "$WAREHOUSE_PID_FILE" ] || [ ! -r "$WAREHOUSE_PID_FILE" ]; then
    die "$ERR_WAREHOUSE_DOWN" "Error: PID file '$WAREHOUSE_PID_FILE' missing or unreadable. Run ./bootstrap.sh"
fi

WAREHOUSE_PID=$(cat "$WAREHOUSE_PID_FILE" 2>/dev/null)
if [ -z "$WAREHOUSE_PID" ]; then
    die "$ERR_WAREHOUSE_DOWN" "Error: PID file '$WAREHOUSE_PID_FILE' is empty (warehouse startup interrupted?)."
fi
if ! kill -0 "$WAREHOUSE_PID" 2>/dev/null; then
    die "$ERR_WAREHOUSE_DOWN" "Error: warehouse not running (PID $WAREHOUSE_PID not active)."
fi

if [ ! -p "$ORDERS_FIFO" ]; then
    die "$ERR_WAREHOUSE_DOWN" "Error: FIFO for orders '$ORDERS_FIFO' does not exist (warehouse not ready?)."
fi


if [ ! -x "$HELPER" ]; then
    die "$ERR_IO" "Error: '$HELPER' not found or not executable (compile with: make build)."
fi

# ---- delegate the binary IPC to the C helper ----
"$HELPER" "$CLIENT_ID" "$ITEM_ID" "$QUANTITY"
exit $?