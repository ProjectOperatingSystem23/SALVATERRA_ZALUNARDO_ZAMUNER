#!/bin/bash
# =============================================================================
# bootstrap.sh -- Launch the Fulfillment Center.
#
# Usage:
#   ./bootstrap.sh <num_receivers> <num_pickers> <num_packers>
#                  <queue_capacity> <num_suppliers> <inventory.csv>
#
# Prepares the environment and starts the C processes in background, then exits
# leaving them running: validate args + CSV, check executables, clean any
# previous run, create the FIFOs, generate supplier_N.conf (round-robin), launch
# ./warehouse and the ./supplier processes (saving PIDs), print a recap.
# =============================================================================

# ===========================================================================
# ERROR / CLEANUP UTILITIES
# ===========================================================================

# err: message to stderr.
err() {
    printf '%s\n' "$*" >&2
}

# die: print the error and exit with code 1.
die() {
    err "$*"
    exit 1
}

# ---- Paths ----
ORDERS_FIFO="/tmp/orders_fifo"          # order.sh -> warehouse
RESTOCK_FIFO="/tmp/restock_fifo"        # supplier/manage.sh -> warehouse
STATUS_FILE="/tmp/wh_status.tmp"        # SIGUSR1 dump: warehouse -> manage.sh
WAREHOUSE_PID_FILE="/tmp/warehouse.pid" # warehouse PID
SUPPLIERS_PID_FILE="/tmp/suppliers.pid" # supplier PIDs (one per line)
LOG_FILE="orders.log"
CONF_DIR="./supplier_configs"           # holds the supplier_N.conf files

# ---- Bootstrap state: decides WHETHER to clean up on error ----
STARTED_PIDS=""        # PIDs already launched (to kill on failure)
RUNTIME_CREATED=0      # 1 = started creating FIFOs/state files
BOOTSTRAP_SUCCESS=0    # 1 = launch completed: do NOT clean up on exit

# cleanup_runtime: Kills launched processes and removes created
# resources. Used only if startup fails halfway.
cleanup_runtime() {
    err "Cleanup after failed launch..."

    for pid in $STARTED_PIDS; do
        kill -TERM "$pid" 2>/dev/null
    done

    rm -f "$ORDERS_FIFO" "$RESTOCK_FIFO" "$STATUS_FILE" "$WAREHOUSE_PID_FILE" "$SUPPLIERS_PID_FILE"

    rm -rf "$CONF_DIR"
}

# on_exit: EXIT trap handler. Runs on every exit; cleans up only if startup did
# not complete but runtime resources were already created.
on_exit() {
    rc=$?

    trap - EXIT INT TERM   # disarm to avoid re-entering on the final exit

    if [ "$BOOTSTRAP_SUCCESS" -ne 1 ] && [ "$RUNTIME_CREATED" -eq 1 ]; then
        cleanup_runtime
    fi

    exit "$rc"
}

# ---- Traps  ----
trap on_exit EXIT
trap 'err "Interrupted by a signal."; exit 1' INT TERM

# ===========================================================================
# ARGUMENT VALIDATION
# ===========================================================================

if [ $# -ne 6 ]; then
    die "Usage: $0 <num_receivers> <num_pickers> <num_packers> <queue_capacity> <num_suppliers> <inventory.csv>"
fi

NUM_RECEIVERS=$1
NUM_PICKERS=$2
NUM_PACKERS=$3
QUEUE_CAPACITY=$4
NUM_SUPPLIERS=$5
CSV_FILE=$6

# The 5 numeric parameters must be strictly positive integers (>= 1).
for arg in "$NUM_RECEIVERS" "$NUM_PICKERS" "$NUM_PACKERS" "$QUEUE_CAPACITY" "$NUM_SUPPLIERS"; do
    case "$arg" in
        ''|*[!0-9]*) die "Error: '$arg' is not a positive integer" ;;  # empty/non-digit
        *[1-9]*)     : ;;                                                # has a 1-9 digit -> OK
        *)           die "Error: '$arg' must be >= 1" ;;            # all digits are 0
    esac
done

# ===========================================================================
# EXECUTABLES VALIDATION
# ===========================================================================

for exe in ./warehouse ./supplier ./order_helper ./manage_restock_helper; do
    if [ ! -f "$exe" ] || [ ! -x "$exe" ]; then
        die "Error: $exe not found/executable (make build)"
    fi
done

# ===========================================================================
# INVENTORY FORMAT VALIDATION (inventory.csv)
# ===========================================================================

if [ ! -f "$CSV_FILE" ] || [ ! -r "$CSV_FILE" ]; then
    die "Error: '$CSV_FILE' not found/readable."
fi

# At least 2 lines: header + one data line.
NUM_LINES=$(grep -c '' "$CSV_FILE")
if [ "$NUM_LINES" -lt 2 ]; then
    die "Error: the CSV must have the header and at least one line of data."
fi

# No duplicate ItemID.
DUPLICATES=$(tail -n +2 "$CSV_FILE" | cut -d',' -f1 | tr -d '\r' | sort | uniq -d)
if [ -n "$DUPLICATES" ]; then
    err "Error: duplicate ItemID found:"
    printf '%s\n' "$DUPLICATES" >&2
    exit 1
fi

# Per-line checks.
LINE_NUM=0
while IFS= read -r line || [ -n "$line" ]; do
    line=${line%$'\r'}
    LINE_NUM=$(( LINE_NUM + 1 ))

    # Line 1 = header: must match exactly.
    if [ "$LINE_NUM" -eq 1 ]; then
        if [ "$line" != "ItemID,Description,Category,Stock" ]; then
            die "Error: invalid header. Expected 'ItemID,Description,Category,Stock', found '$line'"
        fi
        continue
    fi

    if [ -z "$line" ]; then
        die "Error: line $LINE_NUM is empty."
    fi

    # Exactly 4 comma-separated fields.
    NUM_FIELDS=$(awk -F',' '{print NF}' <<< "$line")
    if [ "$NUM_FIELDS" -ne 4 ]; then
        die "Error: line $LINE_NUM has $NUM_FIELDS fields (expected 4)."
    fi

    # No empty field.
    for col in 1 2 3 4; do
        FIELD=$(printf '%s\n' "$line" | cut -d',' -f"$col")
        if [ -z "$FIELD" ]; then
            die "Error: line $LINE_NUM, column $col is empty."
        fi
    done

    ITEM_ID=$(printf '%s\n' "$line" | cut -d',' -f1)
    DESCRIPTION=$(printf '%s\n' "$line" | cut -d',' -f2)
    CATEGORY=$(printf '%s\n' "$line" | cut -d',' -f3)
    STOCK=$(printf '%s\n' "$line" | cut -d',' -f4)

    # ItemID and Stock must be non-negative integers.
    case "$ITEM_ID" in
        ''|*[!0-9]*) die "Error: line $LINE_NUM, ItemID is not a number." ;;
    esac
    case "$STOCK" in
        ''|*[!0-9]*) die "Error: line $LINE_NUM, Stock is not a number." ;;
    esac

    # Lengths consistent with the structs (common.h).
    if [ "${#DESCRIPTION}" -ge 128 ]; then
        die "Error: line $LINE_NUM, Description is too long (max 127 charachters)."
    fi
    if [ "${#CATEGORY}" -ge 64 ]; then
        die "Error: line $LINE_NUM, Category is too long (max 63 charachters)."
    fi
done < "$CSV_FILE"

# ===========================================================================
# CLEAN UP ANY PREVIOUS RUN
# ===========================================================================

if [ -f "$WAREHOUSE_PID_FILE" ]; then
    OLD_PID=$(cat "$WAREHOUSE_PID_FILE") || die "Error: failed to read $WAREHOUSE_PID_FILE"

    if kill -0 "$OLD_PID" 2>/dev/null; then
        err "Error: a warehouse is already executing (PID $OLD_PID)."
        err "Use ./manage.sh shutdown before launching a new instance."
        exit 1
    fi
fi

# From here we modify runtime state: the EXIT trap must clean up on failure.
RUNTIME_CREATED=1

rm -f "$ORDERS_FIFO" "$RESTOCK_FIFO" "$STATUS_FILE" "$WAREHOUSE_PID_FILE" "$SUPPLIERS_PID_FILE" || die "Error: failed to remove old runtime files"

# Remove the previous log so the warehouse (O_APPEND) restarts empty and report
# analyses only the current run.
rm -f "$LOG_FILE" || die "Error: failed to remove the old $LOG_FILE"

# Orphan private client FIFOs from a half-killed previous run.
rm -f /tmp/order_resp_*

rm -rf "$CONF_DIR"   || die "Error: failed to remove the previous $CONF_DIR"
mkdir -p "$CONF_DIR" || die "Error: failed to create the directory $CONF_DIR"

# ===========================================================================
# IPC CREATION (FIFO)
# ===========================================================================

mkfifo "$ORDERS_FIFO"  || die "Error: failed to create $ORDERS_FIFO"
mkfifo "$RESTOCK_FIFO" || die "Error: failed to create $RESTOCK_FIFO"

# ===========================================================================
# GENERATE THE supplier_N.conf FILES
# ===========================================================================

# Each .conf: header + N item lines (item_id,quantity_per_shipment,interval_seconds).
# Every item must be covered by at least one supplier; intervals stay in 5..15 s.
RESTOCK_QTY=5
INTERVAL_MIN=5
INTERVAL_MAX=15
INTERVAL_RANGE=$(( INTERVAL_MAX - INTERVAL_MIN + 1 ))

# Extra suppliers (NUM_SUPPLIERS > NUM_ITEMS) deliver at a lower rate (16-30 s),
# in order not to congest the RESTOCK_FIFO
EXTRA_INTERVAL_MIN=$(( INTERVAL_MAX + 1 ))
EXTRA_INTERVAL_MAX=$(( INTERVAL_MAX * 2 ))
EXTRA_INTERVAL_RANGE=$(( EXTRA_INTERVAL_MAX - EXTRA_INTERVAL_MIN + 1 ))

NUM_ITEMS=$(( NUM_LINES - 1 ))

# Header of each supplier_N.conf.
for ((i = 1; i <= NUM_SUPPLIERS; i++)); do
    CONF_FILE="$CONF_DIR/supplier_${i}.conf"
    printf 'item_id,quantity_per_shipment,interval_seconds\n' > "$CONF_FILE" || die "Error: failed to write on $CONF_FILE"
done

# Round-robin: each item goes to a supplier, cycling 1..NUM_SUPPLIERS.
SUPPLIER_IDX=1
LINE_NUM=0
while IFS= read -r line || [ -n "$line" ]; do
    line=${line%$'\r'}
    LINE_NUM=$(( LINE_NUM + 1 ))
    [ "$LINE_NUM" -eq 1 ] && continue  # skip header

    ITEM_ID=$(printf '%s\n' "$line" | cut -d',' -f1)
    INTERVAL=$(( (RANDOM % INTERVAL_RANGE) + INTERVAL_MIN ))

    printf '%s,%s,%s\n' "$ITEM_ID" "$RESTOCK_QTY" "$INTERVAL" >> "$CONF_DIR/supplier_${SUPPLIER_IDX}.conf" || die "Error: failed to update supplier_${SUPPLIER_IDX}.conf"

    SUPPLIER_IDX=$(( SUPPLIER_IDX + 1 ))
    [ "$SUPPLIER_IDX" -gt "$NUM_SUPPLIERS" ] && SUPPLIER_IDX=1   # wrap-around
done < "$CSV_FILE"

# More suppliers than items: assign a random item to each spare supplier with a longer restock interval(16-30s).
if [ "$NUM_SUPPLIERS" -gt "$NUM_ITEMS" ]; then
    for ((idx = NUM_ITEMS + 1; idx <= NUM_SUPPLIERS; idx++)); do
        RANDOM_LINE=$(( (RANDOM % NUM_ITEMS) + 1 ))

        RANDOM_ITEM=$(tail -n +2 "$CSV_FILE" | sed -n "${RANDOM_LINE}p"  | tr -d '\r' | cut -d',' -f1)
        [ -z "$RANDOM_ITEM" ] && die "Error: selection of a random item failed for supplier $idx"

        INTERVAL=$(( (RANDOM % EXTRA_INTERVAL_RANGE) + EXTRA_INTERVAL_MIN ))

        printf '%s,%s,%s\n' "$RANDOM_ITEM" "$RESTOCK_QTY" "$INTERVAL"  >> "$CONF_DIR/supplier_${idx}.conf" || die "Error: failed to update supplier_${idx}.conf"
    done
fi

# ===========================================================================
# PROCESS LAUNCH
# ===========================================================================

./warehouse "$NUM_RECEIVERS" "$NUM_PICKERS" "$NUM_PACKERS" "$QUEUE_CAPACITY" "$CSV_FILE" &
WAREHOUSE_PID=$!
STARTED_PIDS="$STARTED_PIDS $WAREHOUSE_PID"

printf '%s\n' "$WAREHOUSE_PID" > "$WAREHOUSE_PID_FILE" || die "Error: failed to write on $WAREHOUSE_PID_FILE"

# Give the warehouse a moment to init (FIFO, CSV), then check it is still alive.
sleep 1
if ! kill -0 "$WAREHOUSE_PID" 2>/dev/null; then
    die "Error: warehouse terminated during startup"
fi

# Empty (or create) the suppliers PID file: one per line.
true > "$SUPPLIERS_PID_FILE" || die "Error: failed to create $SUPPLIERS_PID_FILE"

for ((i = 1; i <= NUM_SUPPLIERS; i++)); do
    ./supplier "$i" "$CONF_DIR/supplier_${i}.conf" &
    SUPPLIER_PID=$!
    STARTED_PIDS="$STARTED_PIDS $SUPPLIER_PID"

    printf '%s\n' "$SUPPLIER_PID" >> "$SUPPLIERS_PID_FILE" || die "Error: failed to write the supplier's PID to $SUPPLIERS_PID_FILE"
done

# Short wait and final check: all processes must be alive.
sleep 0.2
for pid in $STARTED_PIDS; do
    if ! kill -0 "$pid" 2>/dev/null; then
        die "Error: the process $pid was terminated during startup"
    fi
done

# Launch succeeded: keep the processes alive -> do NOT clean up on exit.
BOOTSTRAP_SUCCESS=1
trap - EXIT INT TERM

# ===========================================================================
# RECAP
# ===========================================================================

echo ""
echo "=== Fulfillment Center launched ==="
echo "  Receivers : $NUM_RECEIVERS"
echo "  Pickers   : $NUM_PICKERS"
echo "  Packers   : $NUM_PACKERS"
echo "  Queue cap : $QUEUE_CAPACITY"
echo "  Suppliers : $NUM_SUPPLIERS"
echo "  Inventory : $CSV_FILE ($NUM_ITEMS items)"
echo ""
echo "  Warehouse PID : $(cat "$WAREHOUSE_PID_FILE")"
echo "  Supplier PIDs : $(tr '\n' ' ' < "$SUPPLIERS_PID_FILE")"
echo ""
echo "Interface:"
echo "  ./order.sh <client_id> <item_id> <quantity>"
echo "  ./manage.sh status | restock <id> <qty> | report | shutdown"
echo ""
