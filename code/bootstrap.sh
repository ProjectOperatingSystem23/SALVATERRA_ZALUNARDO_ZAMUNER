#!/bin/bash
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

if (( NUM_RECEIVERS < 1 || NUM_PICKERS < 1 || NUM_PACKERS < 1 || QUEUE_CAP < 1 || NUM_SUPPLIERS < 1 )); then
    echo "Parameters 1 to 5 need to be greater than 0"
    exit 1
fi

if [ ! -f "./warehouse" ] || [ ! -x "./warehouse" ]; then
    echo "Error: ./warehouse file was not found or is not executable"
    exit 1
fi

if [ ! -f "./supplier" ] || [ ! -x "./supplier" ]; then
    echo "Error: ./supplier file was not found or is not executable."
    exit 1
fi
#NOTA:
#Controlla solo warehouse e supplier perché il tuo script avvia direttamente quelli.
#Gli altri (order.sh, ecc.) vengono lanciati a mano dall'utente: se mancano, sarà un problema dell'utente in quel momento, non di bootstrap.sh


#VALIDAZIONE DEL FILE CSV
if [ ! -f "$CSV_FILE" ] || [ ! -r "$CSV_FILE" ]; then
    echo "Error: CSV file '$CSV_FILE' not found or not readable."
    exit 1
fi

if [ ! -s "$CSV_FILE" ]; then
    echo "Error: CSV file '$CSV_FILE' is empty."
    exit 1
fi

NUM_LINES=$(wc -l < "$CSV_FILE")
if [ "$NUM_LINES" -lt 2 ]; then
    echo "Error: CSV file must contain at least one data row below the header."
    exit 1
fi


#FIFO
ORDERS_FIFO="/tmp/orders_queue"
SUPPLIER_FIFO="/tmp/supplier_queue"

rm -f "$ORDERS_FIFO" "$SUPPLIER_FIFO"

mkfifo "$ORDERS_FIFO"   || { echo "Error: failed in creating $ORDERS_FIFO";   exit 1; }
mkfifo "$SUPPLIER_FIFO" || { echo "Error: failed in creating $SUPPLIER_FIFO"; exit 1; }

# ---------------------------------------------------------------------------
# validazione csv (l08)
# ---------------------------------------------------------------------------



# ---------------------------------------------------------------------------
# generazione conf supplier
# gestire tutti i casi, relazione N supplier, e numero di tipi di Item
# ---------------------------------------------------------------------------





# ---------------------------------------------------------------------------
# avvio warehouse e supplier
# ---------------------------------------------------------------------------
PID_FILE="/tmp/pids.txt"

./warehouse "$NUM_RECEIVERS" "$NUM_PICKERS" "$NUM_PACKERS" "$QUEUE_CAP" "$CSV_FILE" &
WAREHOUSE_PID=$!
echo "$WAREHOUSE_PID" > "$PID_FILE"

sleep 1
#NOTA: convenzione sul formato del file contentente PIDs??
#Opzioni: Convenzione sulla posizione - Due file separati -  File unico con etichette

for i in $(seq 1 "$NUM_SUPPLIERS"); do
    ./supplier "$i" "supplier_${i}.conf" &
    SUPPLIER_PID=$!
    echo "$SUPPLIER_PID" >> "$PID_FILE"
    #echo "[bootstrap] Supplier $i avviato (PID: $SUPPLIER_PID)"
done













###CLAUDATE

# -lt = "less than". Tutti i parametri numerici devono essere >= 1.
# Usiamo if separati per dare un messaggio di errore preciso per ogni parametro.
if [ "$NUM_RECEIVERS" -lt 1 ]; then
    echo "Errore: num_receivers deve essere >= 1 (ricevuto: $NUM_RECEIVERS)"
    exit 1
fi
if [ "$NUM_PICKERS" -lt 1 ]; then
    echo "Errore: num_pickers deve essere >= 1 (ricevuto: $NUM_PICKERS)"
    exit 1
fi
if [ "$NUM_PACKERS" -lt 1 ]; then
    echo "Errore: num_packers deve essere >= 1 (ricevuto: $NUM_PACKERS)"
    exit 1
fi
if [ "$QUEUE_CAP" -lt 1 ]; then
    echo "Errore: queue_capacity deve essere >= 1 (ricevuto: $QUEUE_CAP)"
    exit 1
fi
if [ "$NUM_SUPPLIERS" -lt 1 ]; then
    echo "Errore: num_suppliers deve essere >= 1 (ricevuto: $NUM_SUPPLIERS)"
    exit 1
fi



# ---------------------------------------------------------------------------
# 3. VALIDAZIONE DEL FILE CSV
# ---------------------------------------------------------------------------
# Verifichiamo quattro cose:
#   a) il file esiste ed è leggibile
#   b) il file non è vuoto (-s = file esiste e ha dimensione > 0, l07)
#   c) ha almeno una riga di dati oltre all'intestazione
#   d) ogni riga ha esattamente 4 campi e i campi numerici sono numeri

# a) esiste e leggibile
if [ ! -f "$CSV_FILE" ] || [ ! -r "$CSV_FILE" ]; then
    echo "Errore: file CSV '$CSV_FILE' non trovato o non leggibile."
    exit 1
fi

# b) non è vuoto
if [ ! -s "$CSV_FILE" ]; then
    echo "Errore: il file CSV '$CSV_FILE' è vuoto."
    exit 1
fi

# c) almeno una riga dati (wc -l conta le righe, l05)
NUM_LINES=$(wc -l < "$CSV_FILE")
if [ "$NUM_LINES" -lt 2 ]; then
    echo "Errore: il CSV deve avere almeno una riga di dati oltre all'intestazione."
    exit 1
fi

# d) validazione riga per riga
# Usiamo "while read -r line" per leggere il file riga per riga (l08).
# Per ogni riga controlliamo:
#   - che abbia esattamente 4 campi separati da virgola
#   - che ItemID (campo 1) sia un numero
#   - che Stock (campo 4) sia un numero
#
# Il pattern case "$VAR" in ''|*[!0-9]*) è il modo Bash (senza regex)
# per verificare che una stringa sia composta solo da cifre (l08, case).
# ''        = stringa vuota
# *[!0-9]*  = contiene almeno un carattere che NON è una cifra

LINE_NUM=0
while read -r line; do
    LINE_NUM=$(( LINE_NUM + 1 ))

    # Salta la riga di intestazione
    if [ "$LINE_NUM" -eq 1 ]; then
        continue
    fi

    # Conta i campi: sostituisce le virgole con newline e conta le righe
    NUM_FIELDS=$(echo "$line" | tr ',' '\n' | wc -l)
    if [ "$NUM_FIELDS" -ne 4 ]; then
        echo "Errore: riga $LINE_NUM ha $NUM_FIELDS campi (attesi 4): '$line'"
        exit 1
    fi

    # Estrai ItemID (campo 1) e Stock (campo 4)
    ITEM_ID=$(echo "$line" | cut -d',' -f1)
    STOCK=$(echo "$line"   | cut -d',' -f4)

    # Verifica che ItemID sia numerico
    case "$ITEM_ID" in
        ''|*[!0-9]*)
            echo "Errore: riga $LINE_NUM — ItemID '$ITEM_ID' non è un numero intero."
            exit 1
            ;;
    esac

    # Verifica che Stock sia numerico
    case "$STOCK" in
        ''|*[!0-9]*)
            echo "Errore: riga $LINE_NUM — Stock '$STOCK' non è un numero intero."
            exit 1
            ;;
    esac

done < "$CSV_FILE"

NUM_ITEMS=$(( NUM_LINES - 1 ))
echo "[bootstrap] CSV validato: $NUM_ITEMS articoli trovati."

# Garanzia di copertura: ogni supplier deve avere almeno un articolo.
# Se NUM_SUPPLIERS > NUM_ITEMS il RR lascerebbe alcuni supplier senza articoli.
# In quel caso riduciamo automaticamente NUM_SUPPLIERS a NUM_ITEMS.
if [ "$NUM_SUPPLIERS" -gt "$NUM_ITEMS" ]; then
    echo "Attenzione: num_suppliers ($NUM_SUPPLIERS) > articoli nel CSV ($NUM_ITEMS)."
    echo "Riduco automaticamente a $NUM_ITEMS supplier per garantire copertura completa."
    NUM_SUPPLIERS=$NUM_ITEMS
fi


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
# 6. AVVIO WAREHOUSE
# ---------------------------------------------------------------------------
# "&" manda il processo in background (l03).
# "$!" cattura il PID dell'ultimo processo avviato in background (l03).
# Salviamo il PID in un file così manage.sh potrà mandare segnali a warehouse.

PID_FILE="/tmp/fc_pids"

./warehouse "$NUM_RECEIVERS" "$NUM_PICKERS" "$NUM_PACKERS" "$QUEUE_CAP" "$CSV_FILE" &
WAREHOUSE_PID=$!
echo "$WAREHOUSE_PID" > "$PID_FILE"
echo "[bootstrap] Warehouse avviato (PID: $WAREHOUSE_PID)"

# Attende 1 secondo per dare a warehouse il tempo di aprire le FIFO.
# Senza questo sleep, i supplier troverebbero le FIFO chiuse e bloccherebbero
# nell'open() perché nessuno ha ancora aperto l'altro lato.
sleep 1

# ---------------------------------------------------------------------------
# 7. AVVIO SUPPLIER
# ---------------------------------------------------------------------------
# Avvia NUM_SUPPLIERS processi supplier in background.
# Ogni supplier riceve: il proprio ID e il suo file di configurazione.
# I PID vengono aggiunti al file PID_FILE (append con >>).

for i in $(seq 1 "$NUM_SUPPLIERS"); do
    ./supplier "$i" "supplier_${i}.conf" &
    SUPPLIER_PID=$!
    echo "$SUPPLIER_PID" >> "$PID_FILE"
    #echo "[bootstrap] Supplier $i avviato (PID: $SUPPLIER_PID)"
done

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




#VALIDAZIONE CSV

# 1. Il file non è vuoto
if [ ! -s "$CSV_FILE" ]; then
    echo "Errore: il file CSV '$CSV_FILE' è vuoto."
    exit 1
fi

# 2. Ha almeno una riga di dati oltre all'intestazione
NUM_LINES=$(wc -l < "$CSV_FILE")
if [ "$NUM_LINES" -lt 2 ]; then
    echo "Errore: il CSV deve avere almeno una riga di dati."
    exit 1
fi

# 3. Ogni riga ha esattamente 4 campi separati da virgola
LINE_NUM=0
while read -r line; do
    LINE_NUM=$(( LINE_NUM + 1 ))
    # Salta intestazione
    if [ $LINE_NUM -eq 1 ]; then
        continue
    fi
    NUM_FIELDS=$(echo "$line" | cut -d',' -f1-10 | tr ',' '\n' | wc -l)
    if [ "$NUM_FIELDS" -ne 4 ]; then
        echo "Errore: riga $LINE_NUM malformata ('$line'), attesi 4 campi."
        exit 1
    fi
    # 4. Il primo campo (ItemID) è numerico
    ITEM_ID=$(echo "$line" | cut -d',' -f1)
    case "$ITEM_ID" in
        ''|*[!0-9]*)
            echo "Errore: riga $LINE_NUM — ItemID '$ITEM_ID' non è un numero."
            exit 1
            ;;
    esac
    # 5. Il quarto campo (Stock) è numerico
    STOCK=$(echo "$line" | cut -d',' -f4)
    case "$STOCK" in
        ''|*[!0-9]*)
            echo "Errore: riga $LINE_NUM — Stock '$STOCK' non è un numero."
            exit 1
            ;;
    esac
done < "$CSV_FILE"

echo "[bootstrap] CSV validato: $((NUM_LINES - 1)) articoli trovati."