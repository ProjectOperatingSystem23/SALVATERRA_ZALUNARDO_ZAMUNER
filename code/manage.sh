#!/bin/bash
# =============================================================================
# manage.sh  --  Gestione del Fulfillment Center (Project 2026-3)
#
# Uso (interfaccia obbligatoria, spec 2.3):
#   ./manage.sh status                 # invia sigusr1 , warehouse scrive status file, questo processo lo legge e stampa a stdout (processi, code, inventario)
#   ./manage.sh restock <item_id> <qty># invia un restock manuale via IPC
#   ./manage.sh report                 # statistiche da orders.log
#   ./manage.sh shutdown               # arresto pulito + cleanup IPC
#
# RUOLO (spec 2.2.8): e' il pannello di controllo del sistema. Comunica con il
# warehouse tramite SEGNALI (Lab03) e IPC (Lab06):
#   - status  : manda SIGUSR1 al warehouse -> il warehouse scrive lo stato in
#               STATUS_FILE; lo script lo legge e lo formatta. Il numero di
#               processi vivi e' verificato con "kill -0" (un segnale "nullo"
#               che non viene consegnato: serve solo a sapere se il PID esiste).
#   - restock : delega l'IPC binario all'helper C ./manual_restock (Bash non sa
#               scrivere struct sulle FIFO), che scrive un RestockMsg sulla
#               RESTOCK_FIFO con supplier_id=0 (= restock manuale, common.h).
#   - report  : analizza orders.log con grep/cut/sort/uniq/awk/wc (Lab09).
#   - shutdown: SIGTERM a supplier e warehouse; il warehouse completa gli ordini
#               in volo e rimuove le sue FIFO; lo script ripulisce il resto.
#
# Riferimenti: Lab03 (segnali, kill -0/-USR1/-TERM), Lab06 (FIFO via helper C),
#              Lab07/08 (scripting: case, funzioni, local), Lab09 (grep/awk/...).
#
# NB path/codici: ricopiati da common.h (in Bash non si puo' #include un .h):
# se cambi un path o un codice in common.h, aggiornalo anche qui (e in
# order.sh / bootstrap.sh).
# =============================================================================

# ---- codici d'errore (IDENTICI a common.h, spec 2.2.9) ----
ERR_OK=0
ERR_IO=4
ERR_WAREHOUSE_DOWN=6
ERR_TIMEOUT=7
ERR_USAGE=8

# ---- path (coerenti con common.h e bootstrap.sh) ----
ORDERS_FIFO="/tmp/orders_fifo"
RESTOCK_FIFO="/tmp/restock_fifo"
STATUS_FILE="/tmp/wh_status.tmp"
WAREHOUSE_PID_FILE="/tmp/warehouse.pid"
SUPPLIERS_PID_FILE="/tmp/suppliers.pid"
LOG_FILE="orders.log"
CONF_DIR="./supplier_configs"
RESTOCK_HELPER="./manual_restock"

# err: messaggio su stderr (fd 2), convenzione Unix (Lab05).
err() { printf '%s\n' "$*" >&2; }

# die <exit_code> <messaggio...>: stampa l'errore ed esce col codice ERR_*.
# Stessa forma di order.sh: il PRIMO argomento e' il codice (spec 2.2.9).
#local rende la variabile locale alla funzione
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

# ---------------------------------------------------------------------------
# Restituisce il PID del warehouse se e' VIVO, altrimenti stringa vuota.
# kill -0 non invia segnali: verifica solo l'esistenza del processo (Lab03).
# ---------------------------------------------------------------------------
warehouse_pid_if_alive() { #la usa solo cmd_status
    { [ -f "$WAREHOUSE_PID_FILE" ] && [ -r "$WAREHOUSE_PID_FILE" ]; }|| return 1
    local pid
    pid=$(cat "$WAREHOUSE_PID_FILE" 2>/dev/null)
    [ -n "$pid" ] || return 1
    kill -0 "$pid" 2>/dev/null || return 1
    printf '%s' "$pid" #chiameremo $(<comando>) che cattura lo std out del comando
    return 0
}

# ===========================================================================
# status
# ===========================================================================
cmd_status() {
    local wpid
    wpid=$(warehouse_pid_if_alive) || wpid="" #$(....) cattura il stdout

    echo "=== Processes status ==="
    if [ -n "$wpid" ]; then #check stringa ! da 0
        echo "  Warehouse : ACTIVE (PID $wpid)"
    else
        echo "  Warehouse : INACTIVE"
    fi

    # Conteggio supplier vivi (kill -0 su ogni PID salvato da bootstrap.sh).
    local total=0 running=0 spid
    if [ -f "$SUPPLIERS_PID_FILE" ] && [ -r "$SUPPLIERS_PID_FILE" ]; then
        while IFS= read -r spid || [ -n "$spid" ]; do #IFS="" vuol dire non separare nulla poi gli altri comandi leggono tutta la riga
            [ -z "$spid" ] && continue #-z = !-n
            total=$(( total + 1 ))
            kill -0 "$spid" 2>/dev/null && running=$(( running + 1 ))
        done < "$SUPPLIERS_PID_FILE"
    fi
    echo "  Suppliers : $running active / $total total"

    # Senza warehouse non possiamo avere code/inventario (li dumpa lui).
    if [ -z "$wpid" ]; then
        echo
        echo "(Queues and inventory unavailable: warehouse is down.)"
        return "$ERR_WAREHOUSE_DOWN"
    fi

        # Chiediamo il dump: rimuoviamo il vecchio file e mandiamo SIGUSR1. Il
        # warehouse scrive su un file temporaneo e poi fa rename() ATOMICO su
        # STATUS_FILE: quando il file COMPARE e' gia' completo, quindi basta
        # aspettarne la comparsa (niente piu' check di completezza del dump).
    rm -f "$STATUS_FILE"
    kill -USR1 "$wpid" 2>/dev/null || die "$ERR_WAREHOUSE_DOWN" "Failed to send SIGUSR1."

    local i
    for i in $(seq 1 30); do          # ~3 s di attesa massima
        [ -s "$STATUS_FILE" ] && break #-s il file esiste e ha dimensione >0
        sleep 0.1
    done
    [ -s "$STATUS_FILE" ] || die "$ERR_TIMEOUT" "Warehouse didn't produce the status dump in time."
    #non serve fare controllo -r perché warehouse l ha aperto con permessi 0644
    echo
    echo "=== Queues (items / capacity') ==="
    echo "  Pending   : $(grep '^PENDING_QUEUE='   "$STATUS_FILE" | cut -d= -f2)"
    echo "  Packaging : $(grep '^PACKAGING_QUEUE=' "$STATUS_FILE" | cut -d= -f2)"
    echo
    echo "=== Inventory ==="
    printf "  %-8s %-30s %-14s %8s\n" "ItemID" "Description" "Category" "Stock"
    printf "  %-8s %-30s %-14s %8s\n" "------" "-----------" "--------" "-----"
    # IFS='|' spezza i campi del dump: ITEM|id|desc|cat|stock
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

    # Stesso filtro caratteri di order.sh: interi STRETTAMENTE positivi (>=1).
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
    item_id=$(( 10#$item_id ))   # base 10: "007" non e' ottale
    qty=$(( 10#$qty ))

    # Il warehouse deve essere vivo e la sua FIFO presente (spec 2.2.8).
    warehouse_pid_if_alive >/dev/null || die "$ERR_WAREHOUSE_DOWN" "Error: warehouse not in execution. Launch ./bootstrap.sh"
    [ -p "$RESTOCK_FIFO" ] || die "$ERR_WAREHOUSE_DOWN" "Error: FIFO restock '$RESTOCK_FIFO' nonexistent."
    { [ -f "$RESTOCK_HELPER" ] && [ -x "$RESTOCK_HELPER" ]; } || die "$ERR_IO" "Error: '$RESTOCK_HELPER' not found/executable (compile with: make build)."
    # Delega l'IPC binario all'helper C; il suo $? e' gia' un ERR_*.
    "$RESTOCK_HELPER" "$item_id" "$qty"
    local restock_ec=$?
    if [ "$restock_ec" -eq "$ERR_OK" ]; then
        echo "Restock accepted by the system (item $item_id, +$qty units')."
        echo "Tip: './manage.sh status' to view the update inventory."
    else
        err "Restock failed (code $restock_ec)."
    fi
    return "$restock_ec" #alternativa return $?, eviti di creare var locale restock_ec
}

# ===========================================================================
# report  --  analisi di orders.log (spec 2.2.8: grep, sort, awk, wc)
#
# Formato riga (warehouse.c log_order):
#   ts|order_id|client_id|item_id|qty_req|qty_shipped|qty_rejected|STATUS
#   STATUS in { SHIPPED, PARTIAL, REJECTED }
# ===========================================================================
cmd_report() {
    { [ -f "$LOG_FILE" ] && [ -r "$LOG_FILE" ] ;} || die "$ERR_IO" "Log '$LOG_FILE' not found."

    local total shipped partial rejected units
    total=$(wc -l < "$LOG_FILE")                    # righe totali = ordini elaborati (wc -l, spec 2.2.8)
    shipped=$(grep -c '|SHIPPED$'  "$LOG_FILE")
    partial=$(grep -c '|PARTIAL$'  "$LOG_FILE")
    rejected=$(grep -c '|REJECTED$' "$LOG_FILE")
    units=$(awk -F'|' '{ s += $6 } END { print s + 0 }' "$LOG_FILE") #processa l'input riga per riga, spezzando ogni riga in campi.
                        #s è una var di awk, viene auto-init a 0 automaticamente
    #spezza ogni riga sui |, somma la colonna 6 (qty_shipped) su tutte le righe, stampa il totale (0 se non c'è nulla)
    echo "=== Order REPORT ($LOG_FILE) ==="
    echo "  Total orders processed : $total"
    echo "  Orders fulfilled: $shipped"
    echo "  Orders partially fulfilled (PARTIAL): $partial"
    echo "  Orders rejected       (REJECTED): $rejected"
    echo "  Total Units Shipped     : $units"

    echo
    echo "  Top 5 Most Ordered Items (by number of orders):"
    # Escludo i rifiutati, prendo l'item_id (campo 4), conto e ordino (Lab09).
    grep -vE '\|REJECTED$' "$LOG_FILE" | cut -d'|' -f4 | sort | uniq -c | sort -rn | head -5 | awk '{ printf "    item %-8s -> %s ordini\n", $2, $1 }'
    #1) il grep outputta tutte le righe che NON hanno il pattern, *termina con:* |REJECTED
    #2)il cut, da ogni riga prende il quarto campo, solo gli itemID
    #3)sort ordina le righe, solo per avere quelli uguali vicini
    #4)uniq collassa quelli uguali, flag -c aggiunge un count (eg <count> <itemIDunico>)
    #5)li ordiniamo per conteggio (-n = confronto numerico), in ordine decrescente (-r)
    #6)head prende i  primi 5
    #7) awk formata il suo input, come nel comando (eg "item <itemID>-><conteggio> ordini\n")

    return "$ERR_OK"
}

# ===========================================================================
# shutdown  --  arresto pulito + cleanup IPC (spec 2.2.8)
# ===========================================================================
cmd_shutdown() {
    local acted=0 spid wpid i

    # 1) Prima i supplier: cosi' non arrivano nuovi restock mentre il
    #    warehouse sta drenando gli ordini in volo.
    if [ -f "$SUPPLIERS_PID_FILE" ] && [ -r "$SUPPLIERS_PID_FILE" ]; then
        while IFS= read -r spid || [ -n "$spid" ]; do #read ritorna 0 se legge riga terminata da \n, se becca EOF entriamo nel ciclo se NON abbiamo letto vuoto (-n ==> stringa NON vuota)
            [ -z "$spid" ] && continue #all ultima ciclata questo potrebbe sssere ridondante ma serve per tutte quelle prima
            kill -TERM "$spid" 2>/dev/null && acted=1
        done < "$SUPPLIERS_PID_FILE"
    fi

    # 2) Il warehouse: SIGTERM -> shutdown ordinato (completa gli ordini in
    #    volo e rimuove le sue FIFO). Aspettiamo che esca davvero.
    if [ -f "$WAREHOUSE_PID_FILE" ] && [ -r "$WAREHOUSE_PID_FILE" ]; then
        wpid=$(cat "$WAREHOUSE_PID_FILE" 2>/dev/null)
        if [ -n "$wpid" ] && kill -0 "$wpid" 2>/dev/null; then
            echo "Send SIGTERM to the warehouse (PID $wpid); waiting for orders in flight..."
            kill -TERM "$wpid" 2>/dev/null && acted=1
            for i in $(seq 1 100); do        # ~20 s (picker/packer dormono 1-3s)
                kill -0 "$wpid" 2>/dev/null || break
                sleep 0.2
            done
            kill -0 "$wpid" 2>/dev/null && { err "Warehouse timed out: force SIGKILL."; kill -KILL "$wpid" 2>/dev/null; }
        fi
    fi

    # 3) Cleanup delle risorse IPC e dei file di stato. Il warehouse rimuove
    #    gia' le proprie FIFO all'uscita pulita; qui ripuliamo comunque tutto
    #    (anche il caso "era gia' morto e ha lasciato file orfani"). Spec 2.2.8.
    rm -f "$WAREHOUSE_PID_FILE" "$SUPPLIERS_PID_FILE" "$STATUS_FILE"   "$ORDERS_FIFO" "$RESTOCK_FIFO"
    rm -f /tmp/order_resp_*          # FIFO private dei client eventualmente orfane
    rm -rf "$CONF_DIR"               # supplier_N.conf generati da bootstrap

    [ "$acted" -eq 1 ] && echo "Shutdown complete; IPC resources cleaned up." || echo "No active processes; clean up any remaining resources."
    return "$ERR_OK"
}

# ===========================================================================
# DISPATCH (Lab08: case)
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