# =============================================================================
# Makefile -- Fulfillment Center (Project 2026-3)
#
# Ricalca il "Final Makefile" del Lab02 (sez. 5 "Makefiles"):
#   CC / CFLAGS, SRCS, OBJS = $(SRCS:.c=.o)
#   regola di link    <target>: <obj>   ->  $(CC) $(CFLAGS) -o $@ $^
#   regola pattern    %.o: %.c          ->  $(CC) $(CFLAGS) -c $< -o $@
#   regola "clean"    rm -f $(OBJS) $(TARGET)
# Variabili automatiche: $@ = target, $^ = tutte le dipendenze, $< = prima dip.
#
# Adattamenti al progetto (l'esempio del prof aveva UN solo eseguibile):
#   - 4 eseguibili (interfacce della spec 2.3) invece di un singolo TARGET;
#   - il warehouse e' multi-thread -> link con -pthread (Lab04);
#   - "build" e "run" sono i nomi richiesti dalla spec 2.3 ("build" corrisponde
#     all'"all" dei lab, che resta presente come sinonimo);
#   - "%.o: %.c common.h": aggiunta la dipendenza dall'header condiviso, cosi'
#     se cambia l'interfaccia binaria si ricompila tutto (nei lab il target era
#     un unico file, qui common.h e' incluso da tutti);
#   - "clean" rimuove anche le risorse IPC (FIFO, pid, ...), come chiede la spec 2.3.
# =============================================================================
#1. build dipende da warehouse → controlla warehouse
#2. warehouse dipende da warehouse.o, common.o → controlla entrambi
#3. warehouse.o non esiste o warehouse.c è più recente?
#   → make cerca una regola che produca warehouse.o
#   → trova la pattern rule: %.o: %.c common.h
#   → applica il pattern: warehouse.o ← warehouse.c (e common.h)
#4. Se warehouse.c (o common.h) è più recente di warehouse.o → ricompila
#5. Stessa cosa per common.o
#6. Se almeno uno tra warehouse.o/common.o è stato ricompilato (o warehouse non esiste)
#   → rilinka warehouse

CC = gcc
CFLAGS = -Wall -Wextra -pedantic
SRCS = common.c warehouse.c supplier.c order_client.c restock_client.c
OBJS = common.o warehouse.o supplier.o order_client.o restock_client.o # intermedi: .c -> .o (gcc -c), riferimenti irrisolti
TARGETS = warehouse supplier order_client restock_client # finali: link (gcc -o); common.o non e' un target (no main)
SCRIPTS = bootstrap.sh order.sh manage.sh
ARGS ?= 2 3 3 10 2 inventory.csv
# Target di default. "build" (richiesto dalla spec 2.3) == "all" (nome dei lab).
build: $(TARGETS)
	@chmod +x $(SCRIPTS) 2>/dev/null || true

# --- Link: ogni eseguibile = proprio .o + common.o  ($@ target, $^ dipendenze) ---
warehouse: warehouse.o common.o
	$(CC) $(CFLAGS) -o $@ $^ -pthread

supplier: supplier.o common.o
	$(CC) $(CFLAGS) -o $@ $^

order_client: order_client.o common.o
	$(CC) $(CFLAGS) -o $@ $^

restock_client: restock_client.o common.o
	$(CC) $(CFLAGS) -o $@ $^

# --- Compilazione: ogni .o dal suo .c  ($< = il file .c) ---
%.o: %.c common.h
	$(CC) $(CFLAGS) -c $< -o $@
#% è un pattern matching: quando make cerca come produrre warehouse.o, controlla se esisteuna
#regola con %.o come target. Se la trova, sostituisce % con warehouse ovunque

# --- clean del prof (rm -f $(OBJS) $(TARGET)) + risorse IPC (spec 2.3) ---
clean:
	rm -f $(OBJS) $(TARGETS)
	rm -f /tmp/orders_fifo /tmp/restock_fifo /tmp/order_resp_* \
	      /tmp/wh_status.tmp /tmp/warehouse.pid /tmp/suppliers.pid
	rm -f orders.log
	rm -rf supplier_configs

# --- run: scenario d'esempio (spec 2.3) ---
run: build
	./bootstrap.sh $(ARGS)
