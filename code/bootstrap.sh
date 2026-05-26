#!/bin/bash

# ===========================================================================
# INPUT VALIDATION
# ===========================================================================

if [ $# -ne 6 ]; then
    echo "Usage: $0 <num_receivers> <num_pickers> <num_packers> <queue_capacity> <num_suppliers> <inventory.csv>"
    exit 1
fi

NUM_RECEIVERS=$1
NUM_PICKERS=$2
NUM_PACKERS=$3
QUEUE_CAP=$4
NUM_SUPPLIERS=$5
CSV_FILE=$6

# Verifica che i parametri numerici siano interi strettamente positivi
for arg in "$NUM_RECEIVERS" "$NUM_PICKERS" "$NUM_PACKERS" "$QUEUE_CAP" "$NUM_SUPPLIERS"; do
    case "$arg" in
        ''|*[!0-9]*)
            echo "Error: '$arg' is not a positive integer"
            exit 1
            ;;
        0)
            echo "Error: '$arg' must be >= 1"
            exit 1
            ;;
    esac
done

# ===========================================================================
# EXECUTABLES VALIDATION
# ===========================================================================

# NOTA DIDATTICA: Controlliamo solo warehouse e supplier perché sono gli unici
# avviati direttamente dal bootstrap. Altri script (es. order.sh) verranno gestiti
# manualmente dall'utente durante la simulazione.
if [ ! -f "./warehouse" ] || [ ! -x "./warehouse" ]; then
    echo "Error: ./warehouse not found or not executable"
    exit 1
fi

if [ ! -f "./supplier" ] || [ ! -x "./supplier" ]; then
    echo "Error: ./supplier not found or not executable"
    exit 1
fi

# ===========================================================================
# INVENTORY FORMAT VALIDATION
# ===========================================================================

# 1. Verifica esistenza e permessi di lettura
if [ ! -f "$CSV_FILE" ] || [ ! -r "$CSV_FILE" ]; then
    echo "Errore: '$CSV_FILE' non trovato o non leggibile."
    exit 1
fi

# 2. Verifica che ci sia contenuto oltre all'intestazione (almeno 2 righe totali)
NUM_LINES=$(wc -l < "$CSV_FILE")
if [ "$NUM_LINES" -lt 2 ]; then
    echo "Errore: il CSV deve avere l'header e almeno una riga dati."
    exit 1
fi

# 3. Analisi riga per riga: controllo di integrità del formato
LINE_NUM=0
while read -r line; do
    LINE_NUM=$(( LINE_NUM + 1 ))
    # Validazione formale dell'intestazione alla prima riga
    if [ "$LINE_NUM" -eq 1 ]; then
      if [ "$line" != "ItemID,Description,Category,Stock" ]; then
          echo "Errore: header non valido. Atteso: 'ItemID,Description,Category,Stock', trovato: '$line'"
          exit 1
      fi
      continue;
    fi   # salta header

    # Verifica presenza di righe vuote intermedie o finali
    if [ -z "$line" ]; then
        echo "Errore: riga $LINE_NUM è vuota."
        exit 1
    fi

    # Verifica che la riga contenga esattamente il numero di campi atteso (4 colonne)
    NUM_FIELDS=$(echo "$line" | tr ',' '\n' | wc -l)
    if [ "$NUM_FIELDS" -ne 4 ]; then
        echo "Errore: riga $LINE_NUM ha $NUM_FIELDS campi (attesi 4)."
        exit 1
    fi

    # Verifica analitica che nessun singolo campo sia vuoto (es. campi adiacenti ',,')
    for col in 1 2 3 4; do
        FIELD=$(echo "$line" | cut -d',' -f"$col")
        if [ -z "$FIELD" ]; then
            echo "Errore: riga $LINE_NUM, colonna $col è vuota."
            exit 1
        fi
    done

done < "$CSV_FILE"

# ===========================================================================
# PREVIOUS STATE CLEAN-UP (FIFO, PID file, status file)
# ===========================================================================

ORDERS_FIFO="/tmp/orders_queue"
SUPPLIER_FIFO="/tmp/supplier_queue"
STATUS_FILE="/tmp/wh_status.tmp"              #lo mettiamo per fare pulizia iniziale anche se lo crea warehouse
WAREHOUSE_PID_FILE="/tmp/warehouse.pid"
SUPPLIERS_PID_FILE="/tmp/suppliers.pid"

rm -f "$ORDERS_FIFO" "$SUPPLIER_FIFO" "$STATUS_FILE" "$WAREHOUSE_PID_FILE" "$SUPPLIERS_PID_FILE"

# ===========================================================================
# CONFIGURAZIONE IPC (Named FIFO)
# ===========================================================================

# /tmp è la directory globale di sistema: garantisce un punto di incontro
# comune anche se i processi vengono lanciati da directory diverse.
mkfifo "$ORDERS_FIFO"   || { echo "Error: failed to create $ORDERS_FIFO";   exit 1; }
mkfifo "$SUPPLIER_FIFO" || { echo "Error: failed to create $SUPPLIER_FIFO"; exit 1; }

# STATUS_FILE non va creato qui: viene scritto da warehouse al ricevimento
# di SIGUSR1. Lo abbiamo rimosso sopra solo per eliminare dati di sessioni
# precedenti.

# ===========================================================================
# .CONF FILES GENERATION
# ===========================================================================
# TODO: subdirectory con i file di configurazione
#TODO: vedere come gestire num_supplier>>>(tanto maggiore)num_item
#DA CHIEDERE?

RESTOCK_QTY=5
INTERVAL_MIN=5
INTERVAL_MAX=15
INTERVAL_RANGE=$(( INTERVAL_MAX - INTERVAL_MIN + 1 ))

# NUM_ITEMS = righe totali del CSV meno l'header
NUM_ITEMS=$(( NUM_LINES - 1 ))

# Passo A: pulizia e creazione dei file con l'header
# rm -f con glob elimina anche file orfani di run precedenti con più supplier
rm -f supplier_*.conf
for i in $(seq 1 "$NUM_SUPPLIERS"); do
    echo "item_id,quantity_per_shipment,interval_seconds" > "supplier_${i}.conf"
done

# Passo B: distribuisce gli item del CSV ai supplier in round-robin
# ogni supplier riceve una fetta degli item a turno: 1→sup1, 2→sup2, ..., N→sup1, ...
SUPPLIER_IDX=1
LINE_NUM=0
while read -r line; do
    LINE_NUM=$(( LINE_NUM + 1 ))

    # salta la prima riga (header del CSV)
    if [ "$LINE_NUM" -eq 1 ]; then continue; fi

    # estrae l'ItemID (prima colonna) e genera un intervallo casuale in [5,15]
    ITEM_ID=$(echo "$line" | cut -d',' -f1)
    INTERVAL=$(( (RANDOM % INTERVAL_RANGE) + INTERVAL_MIN ))

    # appende la riga al file del supplier corrente
    echo "$ITEM_ID,$RESTOCK_QTY,$INTERVAL" >> "supplier_${SUPPLIER_IDX}.conf"

    # avanza al supplier successivo; se ha superato l'ultimo ricomincia da 1
    SUPPLIER_IDX=$(( SUPPLIER_IDX + 1 ))
    if [ "$SUPPLIER_IDX" -gt "$NUM_SUPPLIERS" ]; then
        SUPPLIER_IDX=1
    fi
done < "$CSV_FILE"
# Passo C: supplier in eccesso (NUM_SUPPLIERS > NUM_ITEMS)
# i supplier da NUM_ITEMS+1 in poi hanno solo l'header dopo il round-robin.
# Li trasformiamo in fornitori di rinforzo con item casuali e intervalli
# crescenti per non intasare la FIFO di restock:
#   fascia rinforzo      (NUM_ITEMS < idx <= 2*NUM_ITEMS): intervallo [15,30]s
#   fascia molto eccesso (idx > 2*NUM_ITEMS)             : intervallo [30,60]s
if [ "$NUM_SUPPLIERS" -gt "$NUM_ITEMS" ]; then

    DOUBLE_ITEMS=$(( NUM_ITEMS * 2 ))

    # fascia rinforzo: [INTERVAL_MAX, 2*INTERVAL_MAX] = [15,30]s
    BACKUP_MIN=$INTERVAL_MAX
    BACKUP_MAX=$(( INTERVAL_MAX * 2 ))
    BACKUP_RANGE=$(( BACKUP_MAX - BACKUP_MIN + 1 ))

    # fascia molto in eccesso: [2*INTERVAL_MAX, 4*INTERVAL_MAX] = [30,60]s
    SLOW_MIN=$(( INTERVAL_MAX * 2 ))
    SLOW_MAX=$(( INTERVAL_MAX * 4 ))
    SLOW_RANGE=$(( SLOW_MAX - SLOW_MIN + 1 ))

    for idx in $(seq $(( NUM_ITEMS + 1 )) "$NUM_SUPPLIERS"); do

        # sceglie una riga dati casuale tra 1 e NUM_ITEMS
        RANDOM_LINE=$(( (RANDOM % NUM_ITEMS) + 1 ))

        # rillegge il CSV fino alla riga scelta e ne estrae l'ItemID
        LINE_NUM=0
        RANDOM_ITEM=""
        while read -r line; do
            LINE_NUM=$(( LINE_NUM + 1 ))
            if [ "$LINE_NUM" -eq 1 ]; then continue; fi
            DATA_LINE=$(( LINE_NUM - 1 ))
            if [ "$DATA_LINE" -eq "$RANDOM_LINE" ]; then
                RANDOM_ITEM=$(echo "$line" | cut -d',' -f1)
                break
            fi
        done < "$CSV_FILE"

        # assegna l'intervallo in base alla fascia di appartenenza
        if [ "$idx" -gt "$DOUBLE_ITEMS" ]; then
            INTERVAL=$(( (RANDOM % SLOW_RANGE) + SLOW_MIN ))
        else
            INTERVAL=$(( (RANDOM % BACKUP_RANGE) + BACKUP_MIN ))
        fi

        echo "$RANDOM_ITEM,$RESTOCK_QTY,$INTERVAL" >> "supplier_${idx}.conf"

    done
fi

# ===========================================================================
# AVVIO DEI PROCESSI
# ===========================================================================

# Warehouse per primo: deve aprire le FIFO prima che i supplier scrivano
./warehouse "$NUM_RECEIVERS" "$NUM_PICKERS" "$NUM_PACKERS" "$QUEUE_CAP" "$CSV_FILE" &
echo "$!" > "$WAREHOUSE_PID_FILE"

# Attesa di stabilizzazione: warehouse deve aprire le FIFO in lettura
# prima che i supplier tentino di aprirle in scrittura
sleep 1

for i in $(seq 1 "$NUM_SUPPLIERS"); do
    ./supplier "$i" "supplier_${i}.conf" &
    echo "$!" >> "$SUPPLIERS_PID_FILE"
done

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
echo "  Supplier PIDs : $(cat "$SUPPLIERS_PID_FILE" | tr '\n' ' ')"
echo ""
echo "Useful commands:"
echo "  ./order.sh <client_id> <item_id> <quantity>   # invia un ordine"
echo "  ./manage.sh status | restock <id> <qty> | report | shutdown"
echo ""
