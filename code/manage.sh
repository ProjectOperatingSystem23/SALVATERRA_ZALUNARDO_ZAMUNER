#!/bin/bash
# =============================================================================
# manage.sh: Control panel of the Fulfillment Center.
#
# Usage:
#   ./manage.sh status                  # SIGUSR1 -> warehouse dumps status; print it
#   ./manage.sh restock <item_id> <qty> # manual restock via IPC (helper C)
#   ./manage.sh report                  # statistics from orders.log
#   ./manage.sh shutdown                # clean stop + IPC cleanup
#
# Interacts with the warehouse via signals and IPC.
# =============================================================================

# ---- error codes  ----
ERR_OK=0
ERR_ITEM_NOT_FOUND=1
ERR_IO=4
ERR_WAREHOUSE_DOWN=6
ERR_TIMEOUT=7
ERR_USAGE=8

# ---- paths  ----
ORDERS_FIFO="/tmp/orders_fifo"
RESTOCK_FIFO="/tmp/restock_fifo"
STATUS_FILE="/tmp/wh_status.tmp"
WAREHOUSE_PID_FILE="/tmp/warehouse.pid"
SUPPLIERS_PID_FILE="/tmp/suppliers.pid"
LOG_FILE="orders.log"
CONF_DIR="./supplier_configs"
RESTOCK_HELPER="./manage_restock_helper"

# err: message to stderr (fd 2).
err() { printf '%s\n' "$*" >&2; }

# die <exit_code> <message...>: print the error and exit with the code.
die() {
    local code=$1
    shift
    err "$*"
    exit "$code"
}

usage() {
    err "Usage: $0 <operation> [args...]"
    err "  status                 shows processes, queues and inventory"
    err "  restock <item_id> <qty> sends a manual restock"
    err "  report                 displays statistics from $LOG_FILE"
    err "  shutdown               shuts everything down and cleans allocated IPC resources"
    exit "$ERR_USAGE"
}

# Echo the warehouse PID if it is alive, else return non-zero.
warehouse_pid_if_alive() {
    { [ -f "$WAREHOUSE_PID_FILE" ] && [ -r "$WAREHOUSE_PID_FILE" ]; }|| return 1
    local pid
    pid=$(cat "$WAREHOUSE_PID_FILE" 2>/dev/null)
    [ -n "$pid" ] || return 1
    kill -0 "$pid" 2>/dev/null || return 1
    printf '%s' "$pid"
    return 0
}

# Requests a fresh status dump, takes as argument the warehouse PID
# Returns ERR_OK if the dump is found, otherwise ERR_* non-zero
request_status_dump() {
    local wpid=$1 i
    rm -f "$STATUS_FILE"
    kill -USR1 "$wpid" 2>/dev/null || { err "Failed to send SIGUSR1."; return "$ERR_WAREHOUSE_DOWN"; }

    for i in $(seq 1 30); do          # ~3 s max
        [ -s "$STATUS_FILE" ] && return "$ERR_OK"
        sleep 0.1
    done
    [ -s "$STATUS_FILE" ] || { err "Warehouse didn't produce the status dump in time."; return "$ERR_TIMEOUT"; }
    return "$ERR_OK"
}
# ===========================================================================
# status
# ===========================================================================
cmd_status() {
    local wpid
    wpid=$(warehouse_pid_if_alive) || wpid=""

    echo "=== Processes status ==="
    if [ -n "$wpid" ]; then
        echo "  Warehouse : ACTIVE (PID $wpid)"
    else
        echo "  Warehouse : INACTIVE"
    fi

    # Count live suppliers.
    local total=0 running=0 spid
    if [ -f "$SUPPLIERS_PID_FILE" ] && [ -r "$SUPPLIERS_PID_FILE" ]; then
        while IFS= read -r spid || [ -n "$spid" ]; do
            [ -z "$spid" ] && continue
            total=$(( total + 1 ))
            kill -0 "$spid" 2>/dev/null && running=$(( running + 1 ))
        done < "$SUPPLIERS_PID_FILE"
    fi
    echo "  Suppliers : $running active / $total total"

    if [ -z "$wpid" ]; then
        echo
        echo "(Queues and inventory unavailable: warehouse is down.)"
        return "$ERR_WAREHOUSE_DOWN"
    fi

    # Ask for the dump
    request_status_dump "$wpid" || die "$?" "Warehouse didn't produce the status dump in time."

    echo
    echo "=== Queues (items / capacity) ==="
    echo "  Pending   : $(grep '^PENDING_QUEUE='   "$STATUS_FILE" | cut -d= -f2)"
    echo "  Packaging : $(grep '^PACKAGING_QUEUE=' "$STATUS_FILE" | cut -d= -f2)"
    echo
    echo "=== Inventory ==="
    printf "  %-8s %-30s %-14s %8s\n" "ItemID" "Description" "Category" "Stock"
    printf "  %-8s %-30s %-14s %8s\n" "------" "-----------" "--------" "-----"
    # IFS='|' splits the dump fields: ITEM|id|desc|cat|stock
    grep '^ITEM|' "$STATUS_FILE" | while IFS='|' read -r _ id desc cat stock; do
      printf "  %-8s %-30.30s %-14s %8s\n" "$id" "$desc" "$cat" "$stock"
    done

    return "$ERR_OK"
}

# ===========================================================================
# restock <item_id> <qty>
# ===========================================================================
cmd_restock() {
    [ "$#" -eq 2 ] || die "$ERR_USAGE" "Usage: $0 restock <item_id> <quantity>"
    local item_id=$1 qty=$2

    # Same character filter as bootstrap.sh: strictly positive integers (>= 1).
    case "$item_id" in
        ''|*[!0-9]*) die "$ERR_USAGE" "Error: item_id ('$item_id') must be a positive integer." ;;
        *[1-9]*)     : ;;
        *)           die "$ERR_USAGE" "Error: item_id must be >= 1." ;;
    esac
    case "$qty" in
        ''|*[!0-9]*) die "$ERR_USAGE" "Error: quantity ('$qty') must be a positive integer." ;;
        *[1-9]*)     : ;;
        *)           die "$ERR_USAGE" "Error: quantity must be >= 1." ;;
    esac

    # Checks that the warehouse is alive and its FIFO present
    local wpid
    wpid=$(warehouse_pid_if_alive) || die "$ERR_WAREHOUSE_DOWN" "Error: warehouse not in execution. Launch ./bootstrap.sh"
    [ -p "$RESTOCK_FIFO" ] || die "$ERR_WAREHOUSE_DOWN" "Error: FIFO restock '$RESTOCK_FIFO' nonexistent."
    { [ -f "$RESTOCK_HELPER" ] && [ -x "$RESTOCK_HELPER" ]; } || die "$ERR_IO" "Error: '$RESTOCK_HELPER' not found/executable (make build)."

    # --- Check that the ItemID requested for restock is present inside the inventory ---
    request_status_dump "$wpid"
    local dump_ec=$?
    [ "$dump_ec" -eq "$ERR_OK" ] || die "$dump_ec" "Error: could not read warehouse inventory (status dump failed)."
    if ! grep -q "^ITEM|${item_id}|" "$STATUS_FILE"; then
      die "$ERR_ITEM_NOT_FOUND" "Error: item $item_id does not exist in the inventory; restock refused."
    fi

    # Delegates the binary IPC to the C helper.
    "$RESTOCK_HELPER" "$item_id" "$qty"
    local restock_ec=$?
    if [ "$restock_ec" -eq "$ERR_OK" ]; then
        echo "Restock accepted by the system (item $item_id, +$qty units)."
        echo "Tip: './manage.sh status' to view the updated inventory."
    else
        err "Restock failed (code $restock_ec)."
    fi
    return "$restock_ec"
}

# ===========================================================================
# report -- analysis of orders.log
# Line format (warehouse.c log_order):
#   ts|order_id|client_id|item_id|qty_req|qty_shipped|qty_rejected|STATUS
# ===========================================================================
cmd_report() {
    { [ -f "$LOG_FILE" ] && [ -r "$LOG_FILE" ] ;} || die "$ERR_IO" "Log '$LOG_FILE' not found."

    local total shipped partial rejected units
    total=$(wc -l < "$LOG_FILE")                      # total lines = orders processed
    shipped=$(grep -c '|SHIPPED$'  "$LOG_FILE")
    partial=$(grep -c '|PARTIAL$'  "$LOG_FILE")
    rejected=$(grep -c '|REJECTED$' "$LOG_FILE")
    units=0
    while IFS='|' read -r _ _ _ _ _ qty _; do # search qty_shipped (field 6)
        units=$(( units + qty ))
    done < "$LOG_FILE"
    echo "=== Order REPORT ($LOG_FILE) ==="
    echo "  Total orders processed:                $total"
    echo "  Number of orders fulfilled:            $shipped"
    echo "  Number of orders partially fulfilled:  $partial"
    echo "  NUmber of orders rejected:             $rejected"
    echo "  Total Units Shipped:                   $units"
    echo
    echo "  Top 5 Most Ordered Items (by number of orders):"
    # exclude rejected, take item_id (field 4), count, sort decreasing, top 5.
    grep -vE '\|REJECTED$' "$LOG_FILE" | cut -d'|' -f4 | sort | uniq -c | sort -rn | head -5 |
    while read -r count item; do
        printf "  item %-8s -> %s orders\n" "$item" "$count"
    done
    return "$ERR_OK"
}

# ===========================================================================
# shutdown -- clean stop + IPC cleanup
# ===========================================================================
cmd_shutdown() {
    local acted=0 spid wpid i

    # Suppliers first, so no new restock arrives while the warehouse drains.
    if [ -f "$SUPPLIERS_PID_FILE" ] && [ -r "$SUPPLIERS_PID_FILE" ]; then
        while IFS= read -r spid || [ -n "$spid" ]; do
            [ -z "$spid" ] && continue
            kill -TERM "$spid" 2>/dev/null && acted=1
        done < "$SUPPLIERS_PID_FILE"
    fi

    # Warehouse: SIGTERM -> ordered shutdown. Wait for it to actually exit.
    if [ -f "$WAREHOUSE_PID_FILE" ] && [ -r "$WAREHOUSE_PID_FILE" ]; then
        wpid=$(cat "$WAREHOUSE_PID_FILE" 2>/dev/null)
        if [ -n "$wpid" ] && kill -0 "$wpid" 2>/dev/null; then
            echo "Send SIGTERM to the warehouse (PID $wpid); waiting for orders in flight..."
            kill -TERM "$wpid" 2>/dev/null && acted=1
            for i in $(seq 1 100); do        # ~20 s (picker/packer sleep 1-3s)
                kill -0 "$wpid" 2>/dev/null || break
                sleep 0.2
            done
            kill -0 "$wpid" 2>/dev/null && { err "Warehouse timed out: force SIGKILL."; kill -KILL "$wpid" 2>/dev/null; }
        fi
    fi

    # Clean up IPC and state files (also covers orphan files left by a crash).
    rm -f "$WAREHOUSE_PID_FILE" "$SUPPLIERS_PID_FILE" "$STATUS_FILE" "$ORDERS_FIFO" "$RESTOCK_FIFO"
    rm -f /tmp/order_resp_*
    rm -rf "$CONF_DIR"

    [ "$acted" -eq 1 ] && echo "Shutdown complete; IPC resources cleaned up." || echo "No active processes; clean up any remaining resources."
    return "$ERR_OK"
}

# ===========================================================================
# DISPATCH
# ===========================================================================
[ "$#" -ge 1 ] || usage
OP=$1
shift

case "$OP" in
    status)   cmd_status   "$@" ;;
    restock)  cmd_restock  "$@" ;;
    report)   cmd_report   "$@" ;;
    shutdown) cmd_shutdown "$@" ;;
    *)        usage ;;
esac
exit $?
