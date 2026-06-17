#!/bin/bash
# =============================================================================
# bootstrap.sh  --  Avvio del Fulfillment Center (Project 2026-3)
#
# Uso (interfaccia obbligatoria, spec 2.3):
#   ./bootstrap.sh <num_receivers> <num_pickers> <num_packers>
#                  <queue_capacity> <num_suppliers> <inventory.csv>
#
# RUOLO (spec): e' lo script di "regia". Non elabora ordini: prepara l'ambiente
# e accende i processi C, poi esce lasciandoli in esecuzione in background.
# In ordine:
#   1. valida gli argomenti e l'inventory.csv;
#   2. verifica che gli eseguibili siano compilati;
#   3. ripulisce eventuale stato di una run precedente;
#   4. crea le FIFO di IPC (Lab06);
#   5. genera i file supplier_N.conf (round-robin: ogni item e' coperto);
#   6. lancia ./warehouse e i ./supplier, salvando i loro PID;
#   7. stampa un recap e termina (i processi restano vivi).
#
# Riferimenti corso:
#   - Lab06 (IPC): FIFO / named pipe, mkfifo.
#   - Lab07 / Lab08 (scripting): test su file, trap, gestione errori.
#   - Lab03 (segnali): i nomi dei segnali usati dalle trap (INT/TERM/HUP).
#
# NB sui path: sono gli STESSI definiti in common.h. In Bash non si puo' fare
# #include di un header C, quindi le costanti sono ricopiate qui identiche:
# se cambi un path in common.h, cambialo anche qui (e in manage.sh / order.sh).
# =============================================================================

# ===========================================================================
# UTILITY DI ERRORE / CLEANUP
# ===========================================================================

# err: stampa un messaggio su stderr (fd 2), come da convenzione Unix (Lab05).
err() {
    printf '%s\n' "$*" >&2
}

# die: stampa l'errore ed esce con codice 1. Scorciatoia "stampa-e-muori".
die() {
    err "$*"
    exit 1
}

# ---- Costanti: path delle FIFO e dei file di stato (coerenti con common.h) ----
ORDERS_FIFO="/tmp/orders_fifo"          # order.sh            -> warehouse
RESTOCK_FIFO="/tmp/restock_fifo"        # supplier/manage.sh  -> warehouse
STATUS_FILE="/tmp/wh_status.tmp"        # dump SIGUSR1: warehouse -> manage.sh
WAREHOUSE_PID_FILE="/tmp/warehouse.pid" # PID del warehouse
SUPPLIERS_PID_FILE="/tmp/suppliers.pid" # PID dei supplier (uno per riga)
LOG_FILE="orders.log"
CONF_DIR="./supplier_configs"           # cartella con i file supplier_N.conf

# ---- Stato del bootstrap: decide SE ripulire in caso di errore ----
STARTED_PIDS=""        # PID dei processi gia' avviati (da uccidere se si fallisce)
RUNTIME_CREATED=0      # 1 = abbiamo iniziato a creare FIFO/file di stato
BOOTSTRAP_SUCCESS=0    # 1 = avvio completato: NON ripulire all'uscita

# cleanup_runtime: rollback. Uccide i processi gia' lanciati e rimuove le
# risorse create finora. Serve solo se l'avvio fallisce a meta'.
cleanup_runtime() {
    err "Pulizia dopo avvio fallito..."

    for pid in $STARTED_PIDS; do
        kill -TERM "$pid" 2>/dev/null   # ignora "processo gia' morto"
    done

    rm -f "$ORDERS_FIFO" "$RESTOCK_FIFO" "$STATUS_FILE" \
          "$WAREHOUSE_PID_FILE" "$SUPPLIERS_PID_FILE"

    rm -rf "$CONF_DIR"
}

# on_exit: handler della trap EXIT. Eseguito a OGNI uscita dello script,
# qualunque ne sia la causa. $? cattura il codice d'uscita per ripropagarlo.
on_exit() {
    rc=$?

    # Disarmiamo subito le trap: cosi' l'exit finale non rientra in on_exit
    # (evita ricorsione infinita).
    trap - EXIT INT TERM HUP

    # Cleanup SOLO se l'avvio NON e' arrivato in fondo (BOOTSTRAP_SUCCESS=0) E
    # avevamo gia' creato risorse runtime (RUNTIME_CREATED=1). Se tutto e'
    # andato bene, i processi devono restare vivi: niente cleanup.
    if [ "$BOOTSTRAP_SUCCESS" -ne 1 ] && [ "$RUNTIME_CREATED" -eq 1 ]; then
        cleanup_runtime
    fi

    exit "$rc"
}

# ---- Le quattro trap (Lab03 per i segnali, Lab08 per l'uso negli script) ----
#   EXIT -> pseudo-segnale: scatta a ogni uscita. Punto UNICO del cleanup.
#   INT  -> SIGINT  (Ctrl-C da tastiera).               exit 128+2  = 130.
#   TERM -> SIGTERM (kill "gentile", default di kill).  exit 128+15 = 143.
#   HUP  -> SIGHUP  ("hangup"): chiusura del terminale. exit 128+1  = 129.
# I codici 128+N sono la convenzione shell per "terminato dal segnale N".
trap on_exit EXIT
trap 'err "Interrotto (SIGINT)."; exit 130' INT
trap 'err "Terminato (SIGTERM)."; exit 143' TERM
trap 'err "Hangup (SIGHUP)."; exit 129' HUP

# ===========================================================================
# VALIDAZIONE ARGOMENTI
# ===========================================================================

if [ $# -ne 6 ]; then
    die "Uso: $0 <num_receivers> <num_pickers> <num_packers> <queue_capacity> <num_suppliers> <inventory.csv>"
fi

NUM_RECEIVERS=$1
NUM_PICKERS=$2
NUM_PACKERS=$3
QUEUE_CAP=$4
NUM_SUPPLIERS=$5
CSV_FILE=$6

# I 5 parametri numerici devono essere interi STRETTAMENTE positivi (>= 1).
for arg in "$NUM_RECEIVERS" "$NUM_PICKERS" "$NUM_PACKERS" "$QUEUE_CAP" "$NUM_SUPPLIERS"; do
    case "$arg" in
        ''|*[!0-9]*) die "Errore: '$arg' non e' un intero positivo" ;;  # vuoto/non-cifre
        *[1-9]*)     : ;;                                                # ha una cifra 1-9 -> OK
        *)           die "Errore: '$arg' deve essere >= 1" ;;            # tutte cifre ma valore 0
    esac
done

# Normalizzazione in base 10: "0005" o "010" sarebbero altrimenti letti come
# ottali nelle espressioni aritmetiche (( )). Il prefisso 10# forza la base 10.
NUM_RECEIVERS=$((10#$NUM_RECEIVERS))
NUM_PICKERS=$((10#$NUM_PICKERS))
NUM_PACKERS=$((10#$NUM_PACKERS))
QUEUE_CAP=$((10#$QUEUE_CAP))
NUM_SUPPLIERS=$((10#$NUM_SUPPLIERS))

# ===========================================================================
# VALIDAZIONE ESEGUIBILI (spec 2.3: "verify whether the executables are built")
# ===========================================================================
#TODO: controllare che il path di order client (c helper) sia congruente con quello in order.sh
# -f = file regolare, -x = eseguibile (test su file, Lab07).
for exe in ./warehouse ./supplier ./order_client ./restock_client; do #TODO: vedere se facciamo il restock client
    if [ ! -f "$exe" ] || [ ! -x "$exe" ]; then
        die "Errore: $exe non trovato o non eseguibile (compila con: make build)"
    fi
done

# ===========================================================================
# VALIDAZIONE FORMATO INVENTARIO (inventory.csv)
# ===========================================================================

# -r = leggibile.
if [ ! -f "$CSV_FILE" ] || [ ! -r "$CSV_FILE" ]; then
    die "Errore: '$CSV_FILE' non trovato o non leggibile."
fi

# Servono almeno 2 righe: header + almeno una riga di dati.
NUM_LINES=$(grep -c '' "$CSV_FILE")
if [ "$NUM_LINES" -lt 2 ]; then
    die "Errore: il CSV deve avere l'header e almeno una riga dati."
fi

# Nessun ItemID duplicato (colonna 1, escludendo l'header con tail -n +2).
# uniq -d stampa solo i valori che compaiono piu' di una volta.
DUPLICATES=$(tail -n +2 "$CSV_FILE" | cut -d',' -f1 | tr -d '\r' | sort | uniq -d)
if [ -n "$DUPLICATES" ]; then
    err "Errore: ItemID duplicati trovati:"
    printf '%s\n' "$DUPLICATES" >&2
    exit 1
fi

# Controllo riga per riga. "IFS= read -r" e' la forma corretta per leggere
# righe senza che la shell tocchi spazi o backslash; il '|| [ -n "$line" ]'
# legge anche l'ultima riga senza '\n' finale. Lo "${line%$'\r'}" toglie un
# eventuale CR (file salvati su Windows usano CRLF), cosi' i confronti non
# falliscono per un carattere invisibile. NB: warehouse.c tollera gia' il \r.
LINE_NUM=0
while IFS= read -r line || [ -n "$line" ]; do
    line=${line%$'\r'}
    LINE_NUM=$(( LINE_NUM + 1 ))

    # Riga 1 = header: deve essere ESATTAMENTE quello atteso.
    if [ "$LINE_NUM" -eq 1 ]; then
        if [ "$line" != "ItemID,Description,Category,Stock" ]; then
            die "Errore: header non valido. Atteso 'ItemID,Description,Category,Stock', trovato '$line'"
        fi
        continue
    fi

    if [ -z "$line" ]; then
        die "Errore: riga $LINE_NUM e' vuota."
    fi

    # Esattamente 4 campi separati da virgola.
    # (Limite noto: una Description tra virgolette con una virgola dentro
    #  verrebbe contata come piu' campi. L'inventario fornito non ne ha.)
    NUM_FIELDS=$(awk -F',' '{print NF}' <<< "$line")
    if [ "$NUM_FIELDS" -ne 4 ]; then
        die "Errore: riga $LINE_NUM ha $NUM_FIELDS campi (attesi 4)."
    fi

    # Nessun campo vuoto.
    for col in 1 2 3 4; do
        FIELD=$(printf '%s\n' "$line" | cut -d',' -f"$col")
        if [ -z "$FIELD" ]; then
            die "Errore: riga $LINE_NUM, colonna $col e' vuota."
        fi
    done

    ITEM_ID=$(printf '%s\n' "$line" | cut -d',' -f1)
    DESCRIPTION=$(printf '%s\n' "$line" | cut -d',' -f2)
    CATEGORY=$(printf '%s\n' "$line" | cut -d',' -f3)
    STOCK=$(printf '%s\n' "$line" | cut -d',' -f4)

    # ItemID e Stock devono essere interi non negativi.
    case "$ITEM_ID" in
        ''|*[!0-9]*) die "Errore: riga $LINE_NUM, ItemID non numerico." ;;
    esac
    case "$STOCK" in
        ''|*[!0-9]*) die "Errore: riga $LINE_NUM, Stock non numerico." ;;
    esac

    # Lunghezze coerenti con le struct wire-format (common.h):
    # MAX_DESC=128, MAX_CATEGORY=64 (lasciando spazio al '\0' finale).
    if [ "${#DESCRIPTION}" -ge 128 ]; then
        die "Errore: riga $LINE_NUM, Description troppo lunga (max 127 caratteri)."
    fi
    if [ "${#CATEGORY}" -ge 64 ]; then
        die "Errore: riga $LINE_NUM, Category troppo lunga (max 63 caratteri)."
    fi
done < "$CSV_FILE"

# ===========================================================================
# PULIZIA DI UNA EVENTUALE RUN PRECEDENTE
# ===========================================================================

# kill -0 non invia segnali: verifica solo se il processo esiste (Lab03).
if [ -f "$WAREHOUSE_PID_FILE" ]; then
    OLD_PID=$(cat "$WAREHOUSE_PID_FILE") || die "Errore: impossibile leggere $WAREHOUSE_PID_FILE"

    if kill -0 "$OLD_PID" 2>/dev/null; then
        err "Errore: un warehouse e' gia' in esecuzione (PID $OLD_PID)."
        err "Usa ./manage.sh shutdown prima di avviare una nuova istanza."
        exit 1
    fi
fi

# Da qui modifichiamo lo stato runtime: se qualcosa fallisce, la trap EXIT
# deve ripulire. Alziamo il flag.
RUNTIME_CREATED=1

rm -f "$ORDERS_FIFO" "$RESTOCK_FIFO" "$STATUS_FILE" \
      "$WAREHOUSE_PID_FILE" "$SUPPLIERS_PID_FILE" \
    || die "Errore: impossibile rimuovere i vecchi file runtime"

# Log della run precedente: lo rimuoviamo cosi' il warehouse (che apre in
# O_APPEND) riparte da un file vuoto e ./manage.sh report analizza SOLO la
# run corrente, senza mischiare statistiche di run diverse.
rm -f "$LOG_FILE" || die "Errore: impossibile rimuovere il vecchio $LOG_FILE"

# FIFO di risposta orfane: se una run precedente e' stata uccisa a meta',
# in /tmp possono restare le FIFO private dei client (order_resp_<PID>).
# Nessun processo le usa piu': via anche quelle.
rm -f /tmp/order_resp_*

rm -rf "$CONF_DIR"   || die "Errore: impossibile rimuovere la vecchia $CONF_DIR"
mkdir -p "$CONF_DIR" || die "Errore: impossibile creare la cartella $CONF_DIR"

# ===========================================================================
# CREAZIONE IPC (FIFO) -- Lab06
# ===========================================================================

# Named pipe = canale IPC client<->warehouse e supplier<->warehouse. Le crea il
# bootstrap; il warehouse le apre (e le ricrea idempotente, se servisse). Le
# abbiamo appena rimosse sopra, quindi qui vanno create da zero.
mkfifo "$ORDERS_FIFO"  || die "Errore: impossibile creare $ORDERS_FIFO"
mkfifo "$RESTOCK_FIFO" || die "Errore: impossibile creare $RESTOCK_FIFO"

# ===========================================================================
# GENERAZIONE DEI FILE supplier_N.conf
# ===========================================================================

# Formato di ogni .conf: una riga header + N righe item:
#   item_id,quantity_per_shipment,interval_seconds
# Regole (spec 2.2.6): OGNI item deve essere coperto da almeno un supplier;
# intervalli "ragionevoli" -> restiamo SEMPRE nella finestra 5..15 secondi.
RESTOCK_QTY=5           # unita' per spedizione di restock
INTERVAL_MIN=5          # intervallo minimo (secondi)
INTERVAL_MAX=15         # intervallo massimo (secondi)
INTERVAL_RANGE=$(( INTERVAL_MAX - INTERVAL_MIN + 1 ))

NUM_ITEMS=$(( NUM_LINES - 1 ))   # righe dati = righe totali - header

# Header di ogni supplier_N.conf.
for ((i = 1; i <= NUM_SUPPLIERS; i++)); do
    CONF_FILE="$CONF_DIR/supplier_${i}.conf"
    printf 'item_id,quantity_per_shipment,interval_seconds\n' > "$CONF_FILE" \
        || die "Errore: impossibile scrivere $CONF_FILE"
done

# Distribuzione ROUND-ROBIN: ogni item va a un supplier, ciclando
# 1..NUM_SUPPLIERS. Cosi' tutti gli item sono coperti e il carico bilanciato.
SUPPLIER_IDX=1
LINE_NUM=0
while IFS= read -r line || [ -n "$line" ]; do
    line=${line%$'\r'}
    LINE_NUM=$(( LINE_NUM + 1 ))
    [ "$LINE_NUM" -eq 1 ] && continue       # salta l'header

    ITEM_ID=$(printf '%s\n' "$line" | cut -d',' -f1)
    INTERVAL=$(( (RANDOM % INTERVAL_RANGE) + INTERVAL_MIN ))   # 5..15

    printf '%s,%s,%s\n' "$ITEM_ID" "$RESTOCK_QTY" "$INTERVAL" \
        >> "$CONF_DIR/supplier_${SUPPLIER_IDX}.conf" \
        || die "Errore: impossibile aggiornare supplier_${SUPPLIER_IDX}.conf"

    SUPPLIER_IDX=$(( SUPPLIER_IDX + 1 ))
    [ "$SUPPLIER_IDX" -gt "$NUM_SUPPLIERS" ] && SUPPLIER_IDX=1   # wrap-around
done < "$CSV_FILE"

# Se i supplier sono PIU' degli item, il round-robin sopra ha gia' coperto
# ogni item: ai supplier rimasti senza item assegniamo un item casuale
# (fanno da "fornitore ridondante"), sempre con intervallo 5..15s.
if [ "$NUM_SUPPLIERS" -gt "$NUM_ITEMS" ]; then
    for ((idx = NUM_ITEMS + 1; idx <= NUM_SUPPLIERS; idx++)); do
        RANDOM_LINE=$(( (RANDOM % NUM_ITEMS) + 1 ))   # quale riga dati pescare

        # tail -n +2 salta l'header; sed -n "Np" estrae la riga N-esima.
        RANDOM_ITEM=$(tail -n +2 "$CSV_FILE" | sed -n "${RANDOM_LINE}p" \
                        | tr -d '\r' | cut -d',' -f1)
        [ -z "$RANDOM_ITEM" ] && die "Errore: selezione item casuale fallita per supplier $idx"

        INTERVAL=$(( (RANDOM % INTERVAL_RANGE) + INTERVAL_MIN ))

        printf '%s,%s,%s\n' "$RANDOM_ITEM" "$RESTOCK_QTY" "$INTERVAL" \
            >> "$CONF_DIR/supplier_${idx}.conf" \
            || die "Errore: impossibile aggiornare supplier_${idx}.conf"
    done
fi

# ===========================================================================
# AVVIO DEI PROCESSI
# ===========================================================================

# Il warehouse va in background (&). $! e' il PID dell'ultimo comando in bg.
# Lo salviamo in STARTED_PIDS (per il rollback) e nel suo PID file.
./warehouse "$NUM_RECEIVERS" "$NUM_PICKERS" "$NUM_PACKERS" "$QUEUE_CAP" "$CSV_FILE" &
WAREHOUSE_PID=$!
STARTED_PIDS="$STARTED_PIDS $WAREHOUSE_PID"

printf '%s\n' "$WAREHOUSE_PID" > "$WAREHOUSE_PID_FILE" \
    || die "Errore: impossibile scrivere $WAREHOUSE_PID_FILE"

# Diamo al warehouse un istante per inizializzarsi (FIFO, CSV); poi verifichiamo
# che sia ancora vivo. Se e' morto, inutile avviare i supplier.
sleep 1 #TODO: chiedere al franzil se va bene o meglio sleep 2 o altro
if ! kill -0 "$WAREHOUSE_PID" 2>/dev/null; then
    die "Errore: il warehouse e' terminato durante l'avvio"
fi

# Svuotiamo (o creiamo) il file dei PID dei supplier: uno per riga.
# true non fa nulla, il > richiede un comando prima sennò da errore
true > "$SUPPLIERS_PID_FILE" || die "Errore: impossibile creare $SUPPLIERS_PID_FILE"

for ((i = 1; i <= NUM_SUPPLIERS; i++)); do
    ./supplier "$i" "$CONF_DIR/supplier_${i}.conf" &
    SUPP_PID=$!
    STARTED_PIDS="$STARTED_PIDS $SUPP_PID"

    printf '%s\n' "$SUPP_PID" >> "$SUPPLIERS_PID_FILE" \
        || die "Errore: impossibile scrivere il PID del supplier in $SUPPLIERS_PID_FILE"
done

# Breve attesa e controllo finale: tutti i processi devono essere vivi.
sleep 0.2
for pid in $STARTED_PIDS; do
    if ! kill -0 "$pid" 2>/dev/null; then
        die "Errore: il processo $pid e' terminato durante l'avvio"
    fi
done

# Avvio riuscito: i processi devono restare vivi -> NON ripulire all'uscita.
# Alziamo il flag e disarmiamo le trap (l'EXIT trap non fara' piu' cleanup).
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
echo "Comandi utili:"
echo "  ./order.sh <client_id> <item_id> <quantity>      # invia un ordine"
echo "  ./manage.sh status | restock <id> <qty> | report | shutdown"
echo ""