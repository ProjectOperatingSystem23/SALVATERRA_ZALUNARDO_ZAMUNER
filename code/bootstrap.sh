#!/bin/bash

# ===========================================================================
# COMMON ERROR / CLEANUP UTILITIES
# ===========================================================================
#TODO: TOGLIERE GLI ACCAPO MESSI DA CHAT
err() {
    printf '%s\n' "$*" >&2
}

die() {
    err "$*"
    exit 1
}

ORDERS_FIFO="/tmp/orders_fifo"
RESTOCK_FIFO="/tmp/restock_fifo"
STATUS_FILE="/tmp/wh_status.tmp"
WAREHOUSE_PID_FILE="/tmp/warehouse.pid"
SUPPLIERS_PID_FILE="/tmp/suppliers.pid"

CONF_DIR="./supplier_configs"

STARTED_PIDS=""
RUNTIME_CREATED=0
BOOTSTRAP_SUCCESS=0

cleanup_runtime() {
    err "Cleaning up after startup failure..."

    for pid in $STARTED_PIDS; do
        kill -TERM "$pid" 2>/dev/null
    done

    rm -f "$ORDERS_FIFO" "$RESTOCK_FIFO" "$STATUS_FILE" \
          "$WAREHOUSE_PID_FILE" "$SUPPLIERS_PID_FILE"

    rm -rf "$CONF_DIR"
}

on_exit() {
    rc=$?

    # Evita ricorsione se questa funzione chiama exit.
    trap - EXIT INT TERM HUP

    # Cleanup solo se abbiamo iniziato a creare risorse runtime
    # e il bootstrap non è arrivato al successo.
    if [ "$BOOTSTRAP_SUCCESS" -ne 1 ] && [ "$RUNTIME_CREATED" -eq 1 ]; then
        cleanup_runtime
    fi

    exit "$rc"
}
#TODO COSA FANNO LE ULTIME 3 TRAP?? COSA È HUP
trap on_exit EXIT
trap 'err "Interrupted."; exit 130' INT
trap 'err "Terminated."; exit 143' TERM
trap 'err "Hangup."; exit 129' HUP

# ===========================================================================
# INPUT VALIDATION
# ===========================================================================

if [ $# -ne 6 ]; then
    die "Usage: $0 <num_receivers> <num_pickers> <num_packers> <queue_capacity> <num_suppliers> <inventory.csv>"
fi

NUM_RECEIVERS=$1
NUM_PICKERS=$2
NUM_PACKERS=$3
QUEUE_CAP=$4
NUM_SUPPLIERS=$5
CSV_FILE=$6

# Verifica che i parametri numerici siano interi strettamente positivi.
for arg in "$NUM_RECEIVERS" "$NUM_PICKERS" "$NUM_PACKERS" "$QUEUE_CAP" "$NUM_SUPPLIERS"; do
    case "$arg" in
        ''|*[!0-9]*)
            die "Error: '$arg' is not a positive integer"
            ;;
        *[1-9]*)
            # OK: contiene almeno una cifra non zero.
            ;;
        *)
            die "Error: '$arg' must be >= 1"
            ;;
    esac
done

# Normalizzazione base 10: evita ambiguità con valori tipo 0005 o 05770.
NUM_RECEIVERS=$((10#$NUM_RECEIVERS))
NUM_PICKERS=$((10#$NUM_PICKERS))
NUM_PACKERS=$((10#$NUM_PACKERS))
QUEUE_CAP=$((10#$QUEUE_CAP))
NUM_SUPPLIERS=$((10#$NUM_SUPPLIERS))

# ===========================================================================
# EXECUTABLES VALIDATION
# ===========================================================================

if [ ! -f "./warehouse" ] || [ ! -x "./warehouse" ]; then
    die "Error: ./warehouse not found or not executable"
fi

if [ ! -f "./supplier" ] || [ ! -x "./supplier" ]; then
    die "Error: ./supplier not found or not executable"
fi

# ===========================================================================
# INVENTORY FORMAT VALIDATION
# ===========================================================================

if [ ! -f "$CSV_FILE" ] || [ ! -r "$CSV_FILE" ]; then
    die "Errore: '$CSV_FILE' non trovato o non leggibile."
fi

NUM_LINES=$(grep -c '' "$CSV_FILE")
if [ "$NUM_LINES" -lt 2 ]; then
    die "Errore: il CSV deve avere l'header e almeno una riga dati."
fi

DUPLICATES=$(tail -n +2 "$CSV_FILE" | cut -d',' -f1 | sort | uniq -d)
if [ -n "$DUPLICATES" ]; then
    err "Errore: ItemID duplicati trovati:"
    printf '%s\n' "$DUPLICATES" >&2
    exit 1
fi

LINE_NUM=0
while IFS= read -r line || [ -n "$line" ]; do
    LINE_NUM=$(( LINE_NUM + 1 ))

    if [ "$LINE_NUM" -eq 1 ]; then
        if [ "$line" != "ItemID,Description,Category,Stock" ]; then
            die "Errore: header non valido. Atteso: 'ItemID,Description,Category,Stock', trovato: '$line'"
        fi
        continue
    fi

    if [ -z "$line" ]; then
        die "Errore: riga $LINE_NUM è vuota."
    fi

    NUM_FIELDS=$(awk -F',' '{print NF}' <<< "$line")
    if [ "$NUM_FIELDS" -ne 4 ]; then
        die "Errore: riga $LINE_NUM ha $NUM_FIELDS campi (attesi 4)."
    fi

    for col in 1 2 3 4; do
        FIELD=$(printf '%s\n' "$line" | cut -d',' -f"$col")
        if [ -z "$FIELD" ]; then
            die "Errore: riga $LINE_NUM, colonna $col è vuota."
        fi
    done

    ITEM_ID=$(printf '%s\n' "$line" | cut -d',' -f1)
    DESCRIPTION=$(printf '%s\n' "$line" | cut -d',' -f2)
    CATEGORY=$(printf '%s\n' "$line" | cut -d',' -f3)
    STOCK=$(printf '%s\n' "$line" | cut -d',' -f4)

    case "$ITEM_ID" in
        ''|*[!0-9]*)
            die "Errore: riga $LINE_NUM, ItemID non numerico."
            ;;
    esac

    case "$STOCK" in
        ''|*[!0-9]*)
            die "Errore: riga $LINE_NUM, Stock non numerico."
            ;;
    esac

    if [ "${#DESCRIPTION}" -ge 128 ]; then
        die "Errore: riga $LINE_NUM, Description troppo lunga."
    fi

    if [ "${#CATEGORY}" -ge 64 ]; then
        die "Errore: riga $LINE_NUM, Category troppo lunga."
    fi
done < "$CSV_FILE"

# ===========================================================================
# PREVIOUS STATE CLEAN-UP
# ===========================================================================

if [ -f "$WAREHOUSE_PID_FILE" ]; then
    OLD_PID=$(cat "$WAREHOUSE_PID_FILE") || die "Error: cannot read $WAREHOUSE_PID_FILE"

    if kill -0 "$OLD_PID" 2>/dev/null; then
        err "Error: warehouse already running with PID $OLD_PID"
        err "Use ./manage.sh shutdown before starting a new instance."
        exit 1
    fi
fi

# Da qui in poi il bootstrap sta modificando lo stato runtime.
# Se fallisce, l'EXIT trap deve pulire.
RUNTIME_CREATED=1

rm -f "$ORDERS_FIFO" "$RESTOCK_FIFO" "$STATUS_FILE" \
      "$WAREHOUSE_PID_FILE" "$SUPPLIERS_PID_FILE" \
    || die "Error: failed to remove old runtime files"

rm -rf "$CONF_DIR" || die "Error: failed to remove old supplier config directory"
mkdir -p "$CONF_DIR" || die "Error: failed to create supplier config directory: $CONF_DIR"

# ===========================================================================
# IPC CONFIGURATION
# ===========================================================================

mkfifo "$ORDERS_FIFO"  || die "Error: failed to create $ORDERS_FIFO"
mkfifo "$RESTOCK_FIFO" || die "Error: failed to create $RESTOCK_FIFO"

# ===========================================================================
# .CONF FILES GENERATION
# ===========================================================================

RESTOCK_QTY=5
INTERVAL_MIN=5
INTERVAL_MAX=15
INTERVAL_RANGE=$(( INTERVAL_MAX - INTERVAL_MIN + 1 ))

NUM_ITEMS=$(( NUM_LINES - 1 ))

for ((i = 1; i <= NUM_SUPPLIERS; i++)); do
    CONF_FILE="$CONF_DIR/supplier_${i}.conf"
    printf 'item_id,quantity_per_shipment,interval_seconds\n' > "$CONF_FILE" \
        || die "Error: failed to write $CONF_FILE"
done

SUPPLIER_IDX=1
LINE_NUM=0

while IFS= read -r line || [ -n "$line" ]; do
    LINE_NUM=$(( LINE_NUM + 1 ))

    if [ "$LINE_NUM" -eq 1 ]; then
        continue
    fi

    ITEM_ID=$(printf '%s\n' "$line" | cut -d',' -f1)
    INTERVAL=$(( (RANDOM % INTERVAL_RANGE) + INTERVAL_MIN ))

    printf '%s,%s,%s\n' "$ITEM_ID" "$RESTOCK_QTY" "$INTERVAL" \
        >> "$CONF_DIR/supplier_${SUPPLIER_IDX}.conf" \
        || die "Error: failed to append to $CONF_DIR/supplier_${SUPPLIER_IDX}.conf"

    SUPPLIER_IDX=$(( SUPPLIER_IDX + 1 ))
    if [ "$SUPPLIER_IDX" -gt "$NUM_SUPPLIERS" ]; then
        SUPPLIER_IDX=1
    fi
done < "$CSV_FILE"

if [ "$NUM_SUPPLIERS" -gt "$NUM_ITEMS" ]; then
    DOUBLE_ITEMS=$(( NUM_ITEMS * 2 ))

    BACKUP_MIN=$INTERVAL_MAX
    BACKUP_MAX=$(( INTERVAL_MAX * 2 ))
    BACKUP_RANGE=$(( BACKUP_MAX - BACKUP_MIN + 1 ))

    SLOW_MIN=$(( INTERVAL_MAX * 2 ))
    SLOW_MAX=$(( INTERVAL_MAX * 4 ))
    SLOW_RANGE=$(( SLOW_MAX - SLOW_MIN + 1 ))

    for ((idx = NUM_ITEMS + 1; idx <= NUM_SUPPLIERS; idx++)); do
        RANDOM_LINE=$(( (RANDOM % NUM_ITEMS) + 1 ))

        LINE_NUM=0
        RANDOM_ITEM=""

        while IFS= read -r line || [ -n "$line" ]; do
            LINE_NUM=$(( LINE_NUM + 1 ))

            if [ "$LINE_NUM" -eq 1 ]; then
                continue
            fi

            DATA_LINE=$(( LINE_NUM - 1 ))

            if [ "$DATA_LINE" -eq "$RANDOM_LINE" ]; then
                RANDOM_ITEM=$(printf '%s\n' "$line" | cut -d',' -f1)
                break
            fi
        done < "$CSV_FILE"

        if [ -z "$RANDOM_ITEM" ]; then
            die "Error: failed to select random item for supplier $idx"
        fi

        if [ "$idx" -gt "$DOUBLE_ITEMS" ]; then
            INTERVAL=$(( (RANDOM % SLOW_RANGE) + SLOW_MIN ))
        else
            INTERVAL=$(( (RANDOM % BACKUP_RANGE) + BACKUP_MIN ))
        fi

        printf '%s,%s,%s\n' "$RANDOM_ITEM" "$RESTOCK_QTY" "$INTERVAL" \
            >> "$CONF_DIR/supplier_${idx}.conf" \
            || die "Error: failed to append to $CONF_DIR/supplier_${idx}.conf"
    done
fi

# ===========================================================================
# PROCESSES SPAWNING
# ===========================================================================

./warehouse "$NUM_RECEIVERS" "$NUM_PICKERS" "$NUM_PACKERS" "$QUEUE_CAP" "$CSV_FILE" &
WAREHOUSE_PID=$!
STARTED_PIDS="$STARTED_PIDS $WAREHOUSE_PID"

printf '%s\n' "$WAREHOUSE_PID" > "$WAREHOUSE_PID_FILE" \
    || die "Error: failed to write $WAREHOUSE_PID_FILE"

sleep 1

if ! kill -0 "$WAREHOUSE_PID" 2>/dev/null; then
    die "Error: warehouse terminated during startup"
fi

: > "$SUPPLIERS_PID_FILE" || die "Error: failed to create $SUPPLIERS_PID_FILE"

for ((i = 1; i <= NUM_SUPPLIERS; i++)); do
    ./supplier "$i" "$CONF_DIR/supplier_${i}.conf" &
    SUPP_PID=$!

    STARTED_PIDS="$STARTED_PIDS $SUPP_PID"

    printf '%s\n' "$SUPP_PID" >> "$SUPPLIERS_PID_FILE" \
        || die "Error: failed to write supplier PID to $SUPPLIERS_PID_FILE"
done

sleep 0.2

for pid in $STARTED_PIDS; do
    if ! kill -0 "$pid" 2>/dev/null; then
        die "Error: process $pid terminated during startup"
    fi
done

# Da qui in poi il bootstrap è riuscito: non bisogna pulire su EXIT.
BOOTSTRAP_SUCCESS=1
trap - EXIT INT TERM HUP

# ===========================================================================
# RECAP
# ===========================================================================

echo ""
echo "=== Fulfillment Center avviato ==="
echo "  Receivers : $NUM_RECEIVERS"
echo "  Pickers   : $NUM_PICKERS"
echo "  Packers   : $NUM_PACKERS"
echo "  Queue cap : $QUEUE_CAP"
echo "  Suppliers : $NUM_SUPPLIERS"
echo "  Inventory : $CSV_FILE ($NUM_ITEMS items)"
echo ""
echo "  Warehouse PID : $(cat "$WAREHOUSE_PID_FILE")"
echo "  Supplier PIDs : $(tr '\n' ' ' < "$SUPPLIERS_PID_FILE")"
echo ""
echo "Useful commands:"
echo "  ./order.sh <client_id> <item_id> <quantity>   # invia un ordine"
echo "  ./manage.sh status | restock <id> <qty> | report | shutdown"
echo ""