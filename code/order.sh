#!/bin/bash
# =============================================================================
# order.sh  --  Invio di un ordine al Fulfillment Center (Project 2026-3)
#
# Uso (interfaccia obbligatoria, spec 2.3):
#   ./order.sh <client_id> <item_id> <quantity>
#
# RUOLO: e' il front-end Bash dell'ordine. Bash NON sa scrivere/leggere struct
# binarie sulle FIFO, quindi si divide il lavoro (spec 2.2.9):
#   1. valida gli argomenti        (Lab07: test su stringhe, case);
#   2. verifica che il warehouse sia vivo (PID file + kill -0, Lab03);
#   3. delega l'IPC binario all'helper ./order_client (Lab06);
#   4. ripropaga il suo exit code, che e' uno degli ERR_* di common.h.
#
# Perche' un helper C e non "echo > fifo"? Una struct OrderRequest e' un blocco
# BINARIO (interi + buffer a lunghezza fissa): con echo scriveremmo testo, che
# il warehouse interpreterebbe come byte sbagliati. Serve write(2) di
# sizeof(OrderRequest) -> lo fa order_client. La risposta va letta su una FIFO
# privata perche' l'ordine e' SINCRONO: il client resta in attesa dell'esito
# (shipped/partial/rejected) e lo traduce in exit code; due FIFO (richiesta +
# risposta) servono perche' una named pipe e' unidirezionale (Lab06).
#
# Riferimenti: Lab03 (kill -0), Lab06 (FIFO via helper C), Lab07/08 (scripting).
# NB path/codici: ricopiati da common.h (in Bash non si puo' #include un .h):
# se cambi un path o un codice in common.h, aggiornalo anche qui.
# =============================================================================

# ---- codici d'errore (IDENTICI a common.h, spec 2.2.9) ----
ERR_IO=5
ERR_WAREHOUSE_DOWN=8
ERR_USAGE=10

# ---- path (coerenti con common.h e bootstrap.sh) ----
ORDERS_FIFO="/tmp/orders_fifo"
WAREHOUSE_PID_FILE="/tmp/warehouse.pid"

# ---- Helper C (path relativo: va lanciato dalla cartella del progetto) ----
HELPER="./order_client"

# err: messaggio su stderr (fd 2), convenzione Unix (Lab05).
err() { printf '%s\n' "$*" >&2; }

# die: stampa il messaggio ed esce. Come in bootstrap.sh, ma qui il PRIMO
# argomento e' il CODICE ERR_* da restituire (spec 2.2.9: stessi numeri in C e
# Bash); il resto e' il messaggio. Centralizza "stampa errore + exit" cosi' ogni
# punto di uscita d'errore e' una riga sola e i codici restano consistenti.
#   uso: die <exit_code> <messaggio...>
die() {
    code=$1
    shift
    err "$*"
    exit "$code"
}

# ---- uso ----
if [ "$#" -ne 3 ]; then
    die "$ERR_USAGE" "Uso: $0 <client_id> <item_id> <quantity>"
fi

CLIENT_ID=$1
ITEM_ID=$2
QUANTITY=$3

# ---- validazione client_id ----
# Niente '|', spazi o caratteri di controllo: il warehouse scrive client_id in
# orders.log, che e' pipe-separated (timestamp|order_id|client_id|...). Un '|'
# o un a-capo nel client_id corromperebbe il log e le statistiche di report.
# Ammettiamo lettere, cifre, '_', '-', '.' e 1..63 caratteri (MAX_CLIENT_ID-1).
case "$CLIENT_ID" in
    "")
        die "$ERR_USAGE" "Errore: client_id vuoto." ;;
    *[!A-Za-z0-9_.-]*)
        die "$ERR_USAGE" "Errore: client_id contiene caratteri non ammessi (usa A-Z a-z 0-9 _ . -)." ;;
esac
if [ "${#CLIENT_ID}" -ge 64 ]; then
    die "$ERR_USAGE" "Errore: client_id troppo lungo (max 63 caratteri)."
fi

# ---- validazione item_id: intero STRETTAMENTE positivo (>= 1) ----
# ''|*[!0-9]* scarta vuoto e qualsiasi carattere non-cifra (anche segni/spazi);
# *[1-9]*    richiede almeno una cifra 1-9, cosi' "0" / "00" vengono respinti.
# (Stesso pattern usato in bootstrap.sh per i parametri numerici.)
case "$ITEM_ID" in
    ''|*[!0-9]*) die "$ERR_USAGE" "Errore: item_id ('$ITEM_ID') non e' un intero positivo." ;;
    *[1-9]*)     : ;;                                                            # > 0 -> ok
    *)           die "$ERR_USAGE" "Errore: item_id deve essere >= 1." ;;
esac

# ---- validazione quantity: intero STRETTAMENTE positivo (>= 1) ----
case "$QUANTITY" in
    ''|*[!0-9]*) die "$ERR_USAGE" "Errore: quantity ('$QUANTITY') non e' un intero positivo." ;;
    *[1-9]*)     : ;;                                                             # > 0 -> ok
    *)           die "$ERR_USAGE" "Errore: quantity deve essere >= 1." ;;
esac

# Normalizzazione base 10: evita che "007" sia letto come ottale in (( )).
ITEM_ID=$((10#$ITEM_ID))
QUANTITY=$((10#$QUANTITY))

# ---- il warehouse e' vivo? (spec: ERR_WAREHOUSE_DOWN) ----
# kill -0 non invia segnali: verifica solo se il processo esiste (Lab03).
# Tre cause distinte -> tre messaggi distinti (piu' facile da diagnosticare):
#   a) PID file mancante o non leggibile -> warehouse mai avviato;
#   b) PID file presente ma vuoto        -> avvio interrotto a meta';
#   c) PID che non risponde a kill -0    -> processo terminato.
if [ ! -f "$WAREHOUSE_PID_FILE" ] || [ ! -r "$WAREHOUSE_PID_FILE" ]; then
    die "$ERR_WAREHOUSE_DOWN" "Errore: PID file '$WAREHOUSE_PID_FILE' mancante o non leggibile. Avvia ./bootstrap.sh"
fi

WH_PID=$(cat "$WAREHOUSE_PID_FILE" 2>/dev/null)
if [ -z "$WH_PID" ]; then
    die "$ERR_WAREHOUSE_DOWN" "Errore: PID file '$WAREHOUSE_PID_FILE' vuoto (avvio del warehouse interrotto?)."
fi
if ! kill -0 "$WH_PID" 2>/dev/null; then
    die "$ERR_WAREHOUSE_DOWN" "Errore: warehouse non in esecuzione (PID $WH_PID non attivo)."
fi

# -p = la FIFO esiste ed e' una named pipe (Lab06/07).
if [ ! -p "$ORDERS_FIFO" ]; then
    die "$ERR_WAREHOUSE_DOWN" "Errore: FIFO ordini '$ORDERS_FIFO' inesistente (warehouse non pronto?)."
fi

# ---- helper compilato? (-x = eseguibile, Lab07) ----
if [ ! -x "$HELPER" ]; then
    die "$ERR_IO" "Errore: '$HELPER' non trovato o non eseguibile (compila con: make build)."
fi

# ---- delega l'IPC binario all'helper C; il suo $? e' gia' un ERR_* ----
# Niente "exec": vogliamo che il codice di ritorno torni a order.sh e che lo
# script termini con lo stesso valore (utile a manage.sh / a chi concatena).
"$HELPER" "$CLIENT_ID" "$ITEM_ID" "$QUANTITY"
exit $?