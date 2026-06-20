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
ERR_IO=5
ERR_WAREHOUSE_DOWN=8
ERR_TIMEOUT=9
ERR_USAGE=10

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
    err "Uso: $0 <operation> [args...]"
    err "  status                 mostra processi, code e inventario"
    err "  restock <item_id> <qty> invia un restock manuale"
    err "  report                 statistiche da $LOG_FILE"
    err "  shutdown               arresta tutto e ripulisce le risorse IPC"
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

    echo "=== Stato processi ==="
    if [ -n "$wpid" ]; then #check stringa ! da 0
        echo "  Warehouse : ATTIVO (PID $wpid)"
    else
        echo "  Warehouse : NON ATTIVO"
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
    echo "  Suppliers : $running attivi / $total totali"

    # Senza warehouse non possiamo avere code/inventario (li dumpa lui).
    if [ -z "$wpid" ]; then
        echo
        echo "(Code e inventario non disponibili: il warehouse non e' attivo.)"
        return "$ERR_WAREHOUSE_DOWN"
    fi

        # Chiediamo il dump: rimuoviamo il vecchio file e mandiamo SIGUSR1. Il
        # warehouse scrive su un file temporaneo e poi fa rename() ATOMICO su
        # STATUS_FILE: quando il file COMPARE e' gia' completo, quindi basta
        # aspettarne la comparsa (niente piu' check di completezza del dump).
    rm -f "$STATUS_FILE"
    kill -USR1 "$wpid" 2>/dev/null || die "$ERR_WAREHOUSE_DOWN" "Invio di SIGUSR1 fallito."

    local i
    for i in $(seq 1 30); do          # ~3 s di attesa massima
        [ -s "$STATUS_FILE" ] && break #-s il file esiste e ha dimensione >0
        sleep 0.1
    done
    [ -s "$STATUS_FILE" ] || die "$ERR_TIMEOUT" "Il warehouse non ha prodotto lo status in tempo."
    #nons serve fare controllo -r perché warehouse l ha aperto con permessi 0644
    echo
    echo "=== Code (occupazione / capacita') ==="
    echo "  Pending   : $(grep '^PENDING_QUEUE='   "$STATUS_FILE" | cut -d= -f2)"
    echo "  Packaging : $(grep '^PACKAGING_QUEUE=' "$STATUS_FILE" | cut -d= -f2)"
    echo
    echo "=== Inventario ==="
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
    [ "$#" -eq 2 ] || die "$ERR_USAGE" "Uso: $0 restock <item_id> <quantity>"
    local item_id=$1 qty=$2

    # Stesso filtro caratteri di order.sh: interi STRETTAMENTE positivi (>=1).
    case "$item_id" in
        ''|*[!0-9]*) die "$ERR_USAGE" "Errore: item_id ('$item_id') non e' un intero positivo." ;;
        *[1-9]*)     : ;;
        *)           die "$ERR_USAGE" "Errore: item_id deve essere >= 1." ;;
    esac
    case "$qty" in
        ''|*[!0-9]*) die "$ERR_USAGE" "Errore: quantity ('$qty') non e' un intero positivo." ;;
        *[1-9]*)     : ;;
        *)           die "$ERR_USAGE" "Errore: quantity deve essere >= 1." ;;
    esac
    item_id=$(( 10#$item_id ))   # base 10: "007" non e' ottale
    qty=$(( 10#$qty ))

    # Il warehouse deve essere vivo e la sua FIFO presente (spec 2.2.8).
    warehouse_pid_if_alive >/dev/null || die "$ERR_WAREHOUSE_DOWN" "Errore: warehouse non in esecuzione. Avvia ./bootstrap.sh"
    [ -p "$RESTOCK_FIFO" ] || die "$ERR_WAREHOUSE_DOWN" "Errore: FIFO restock '$RESTOCK_FIFO' inesistente (warehouse non pronto?)."
    { [ -f "$RESTOCK_HELPER" ] && [ -x "$RESTOCK_HELPER" ]; } || die "$ERR_IO" "Errore: '$RESTOCK_HELPER' non trovato o non eseguibile (compila con: make build)."
    # Delega l'IPC binario all'helper C; il suo $? e' gia' un ERR_*.
    "$RESTOCK_HELPER" "$item_id" "$qty"
    local rc=$?
    if [ "$rc" -eq "$ERR_OK" ]; then
        echo "Restock accettato dal sistema (item $item_id, +$qty unita')."
        echo "Suggerimento: './manage.sh status' per vedere lo stock aggiornato."
    else
        err "Restock fallito (codice $rc)."
    fi
    return "$rc" #alternativa return $?, eviti di creare var locale rc
}

# ===========================================================================
# report  --  analisi di orders.log (spec 2.2.8: grep, sort, awk, wc)
#
# Formato riga (warehouse.c log_order):
#   ts|order_id|client_id|item_id|qty_req|qty_shipped|qty_rejected|STATUS
#   STATUS in { SHIPPED, PARTIAL, REJECTED }
# ===========================================================================
cmd_report() {
    { [ -f "$LOG_FILE" ] && [ -r "$LOG_FILE" ] ;} || die "$ERR_IO" "Log '$LOG_FILE' non trovato (nessun ordine elaborato?)."

    local total shipped partial rejected units
    total=$(wc -l < "$LOG_FILE")                    # righe totali = ordini elaborati (wc -l, spec 2.2.8)
    shipped=$(grep -c '|SHIPPED$'  "$LOG_FILE")
    partial=$(grep -c '|PARTIAL$'  "$LOG_FILE")
    rejected=$(grep -c '|REJECTED$' "$LOG_FILE")
    units=$(awk -F'|' '{ s += $6 } END { print s + 0 }' "$LOG_FILE") #processa l'input riga per riga, spezzando ogni riga in campi.
                        #s è una var di awk, viene auto-init a 0 automaticamente
    #spezza ogni riga sui |, somma la colonna 6 (qty_shipped) su tutte le righe, stampa il totale (0 se non c'è nulla)
    echo "=== Report ordini ($LOG_FILE) ==="
    echo "  Ordini elaborati (totale) : $total"
    echo "  Spediti completi (SHIPPED): $shipped"
    echo "  Spediti parziali (PARTIAL): $partial"
    echo "  Rifiutati       (REJECTED): $rejected"
    echo "  Unita' totali spedite     : $units"

    echo
    echo "  Top 5 item piu' ordinati (per numero di ordini con spedizione):"
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
            echo "Invio SIGTERM al warehouse (PID $wpid); attendo gli ordini in volo..."
            kill -TERM "$wpid" 2>/dev/null && acted=1
            for i in $(seq 1 100); do        # ~20 s (picker/packer dormono 1-3s)
                kill -0 "$wpid" 2>/dev/null || break
                sleep 0.2
            done
            kill -0 "$wpid" 2>/dev/null && { err "Warehouse non uscito in tempo: forzo SIGKILL."; kill -KILL "$wpid" 2>/dev/null; }
        fi
    fi

    # 3) Cleanup delle risorse IPC e dei file di stato. Il warehouse rimuove
    #    gia' le proprie FIFO all'uscita pulita; qui ripuliamo comunque tutto
    #    (anche il caso "era gia' morto e ha lasciato file orfani"). Spec 2.2.8.
    rm -f "$WAREHOUSE_PID_FILE" "$SUPPLIERS_PID_FILE" "$STATUS_FILE"   "$ORDERS_FIFO" "$RESTOCK_FIFO"
    rm -f /tmp/order_resp_*          # FIFO private dei client eventualmente orfane
    rm -rf "$CONF_DIR"               # supplier_N.conf generati da bootstrap

    [ "$acted" -eq 1 ] && echo "Shutdown completato; risorse IPC ripulite." || echo "Nessun processo attivo; ripulite eventuali risorse residue."
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