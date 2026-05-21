#!/bin/bash

# ---------------------------------------------------------------------------
# CONTRLLO ARGOMENTI DI INGRESSO
# ---------------------------------------------------------------------------

if [ $# -ne 6 ]; then
  echo "Usage $0 <num_receivers> <num_pickers> <num_packers> <queue_capacity> <num_suppliers> <inventory.csv>"
  exit 1
fi

NUM_RECEIVERS=$1
NUM_PICKERS=$2
NUM_PACKERS=$3
QUEUE_CAP=$4
NUM_SUPPLIERS=$5
CSV_FILE=$6

# Verifica che tutti i parametri numerici siano strettamente positivi
if (( NUM_RECEIVERS < 1 || NUM_PICKERS < 1 || NUM_PACKERS < 1 || QUEUE_CAP < 1 || NUM_SUPPLIERS < 1 )); then
    echo "Parameters 1 to 5 need to be greater than 0"
    exit 1
fi

# Verifica la presenza e l'eseguibilità dei binari locali fondamentali
if [ ! -f "./warehouse" ] || [ ! -x "./warehouse" ]; then
    echo "Error: ./warehouse file was not found or is not executable"
    exit 1
fi

if [ ! -f "./supplier" ] || [ ! -x "./supplier" ]; then
    echo "Error: ./supplier file was not found or is not executable."
    exit 1
fi

# TODO: NOTA DIDATTICA: Controlliamo solo warehouse e supplier perché sono gli unici
# avviati direttamente dal bootstrap. Altri script (es. order.sh) verranno gestiti
# manualmente dall'utente durante la simulazione.


# ===========================================================================
# CONFIGURAZIONE IPC (Named Pipe / FIFO)
# ===========================================================================

# Usiamo il percorso assoluto in /tmp perché è una directory globale di sistema.
# Questo garantisce un punto di incontro comune per la comunicazione tra processi,
# anche se warehouse, supplier o order.sh venissero lanciati da cartelle diverse.

ORDERS_FIFO="/tmp/orders_queue"
SUPPLIER_FIFO="/tmp/supplier_queue"

# NOTA SULL'EXPORT: Se i binari C leggono i path delle FIFO da variabili d'ambiente,
# ricordati di scommentare le righe qui sotto per renderle visibili ai processi figli:


# Pulizia di canali residui da esecuzioni precedenti
rm -f "$ORDERS_FIFO" "$SUPPLIER_FIFO"

mkfifo "$ORDERS_FIFO"   || { echo "Error: failed in creating $ORDERS_FIFO";   exit 1; }
mkfifo "$SUPPLIER_FIFO" || { echo "Error: failed in creating $SUPPLIER_FIFO"; exit 1; }

# ===========================================================================
# VALIDAZIONE STRUTTURALE DEL FILE CSV (Inventory)
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
# GENERAZIONE CONFIGURAZIONE FORNITORI (Supplier Conf)
# ===========================================================================

# TODO: Implementare qui la logica di ripartizione degli Item del CSV
# tra gli N Supplier e generare i relativi file "supplier_X.conf"
# ---------------------------------------------------------------------------

# TODO: subdirectory con i file di configurazione
#DA CHIEDERE?


# ===========================================================================
# AVVIO DEI PROCESSI DI SIMULAZIONE (warehouse e supplier su due file separati)
# ===========================================================================
WAREHOUSE_PID_FILE="/tmp/warehouse.pid"
SUPPLIERS_PID_FILE="/tmp/suppliers.pid"

# Pulizia file PID residui da esecuzioni precedenti
rm -f "$WAREHOUSE_PID_FILE" "$SUPPLIERS_PID_FILE"

# Avvio del magazzino in background e salvataggio del suo PID
./warehouse "$NUM_RECEIVERS" "$NUM_PICKERS" "$NUM_PACKERS" "$QUEUE_CAP" "$CSV_FILE" &
WAREHOUSE_PID=$!
echo "$WAREHOUSE_PID" > "$WAREHOUSE_PID_FILE"
#echo "[bootstrap] Warehouse avviato (PID: $WAREHOUSE_PID)"

# Attesa di stabilizzazione per permettere al magazzino di aprire le FIFO
sleep 1

# Avvio dei supplier in background
for i in $(seq 1 "$NUM_SUPPLIERS"); do
    ./supplier "$i" "supplier_${i}.conf" &
    SUPPLIER_PID=$!
    echo "$SUPPLIER_PID" >> "$SUPPLIERS_PID_FILE"
    #echo "[bootstrap] Supplier $i avviato (PID: $SUPPLIER_PID)"
done












#/////////////////////////////////////////////////CLAUDATE//////////////////////////////////////////////////////////////

# ---------------------------------------------------------------------------
# 5. GENERAZIONE FILE DI CONFIGURAZIONE SUPPLIER (Round-Robin)
# ---------------------------------------------------------------------------
RESTOCK_QTY=5             # quantità inviata per ogni ciclo di restock
INTERVAL_MIN=5            # intervallo minimo in secondi
INTERVAL_MAX=15           # intervallo massimo in secondi
INTERVAL_RANGE=$(( INTERVAL_MAX - INTERVAL_MIN + 1 ))   # = 11

# Passo A: inizializza i file con l'intestazione CSV
for i in $(seq 1 "$NUM_SUPPLIERS"); do
    rm -f "supplier_${i}.conf"
    echo "item_id,quantity_per_shipment,interval_seconds" > "supplier_${i}.conf"
    #echo "[bootstrap] Supplier $i: file inizializzato"
done

# Passo B: distribuisce gli articoli in Round-Robin
SUPPLIER_IDX=1
LINE_NUM=0

while read -r line; do
    LINE_NUM=$(( LINE_NUM + 1 ))

    # Salta la riga di intestazione del CSV
    if [ "$LINE_NUM" -eq 1 ]; then
        continue
    fi

    # Estrai ItemID (primo campo del CSV)
    ITEM_ID=$(echo "$line" | cut -d',' -f1)

    # Intervallo casuale per ogni articolo
    INTERVAL=$(( (RANDOM % INTERVAL_RANGE) + INTERVAL_MIN ))

    # Aggiungi l'articolo al file del supplier corrente
    echo "$ITEM_ID,$RESTOCK_QTY,$INTERVAL" >> "supplier_${SUPPLIER_IDX}.conf"

    # Avanza al supplier successivo (Round-Robin)
    SUPPLIER_IDX=$(( SUPPLIER_IDX + 1 ))
    if [ "$SUPPLIER_IDX" -gt "$NUM_SUPPLIERS" ]; then
        SUPPLIER_IDX=1
    fi

done < "$CSV_FILE"

echo "[bootstrap] Configurazione generata per $NUM_SUPPLIERS supplier(s) (Round-Robin)."


# ---------------------------------------------------------------------------
# 8. RIEPILOGO
# ---------------------------------------------------------------------------

echo ""
echo "=== Fulfillment Center avviato ==="
echo "  Receivers  : $NUM_RECEIVERS"
echo "  Pickers    : $NUM_PICKERS"
echo "  Packers    : $NUM_PACKERS"
echo "  Queue cap  : $QUEUE_CAP"
echo "  Suppliers  : $NUM_SUPPLIERS"
echo "  Inventory  : $CSV_FILE ($NUM_ITEMS articoli)"
echo ""
echo "PIDs salvati in: $PID_FILE"
echo "Comandi: ./manage.sh status | ./manage.sh shutdown"

#TODO interfaccia  grafica come piace al fonta