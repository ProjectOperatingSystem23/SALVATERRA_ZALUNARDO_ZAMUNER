# =============================================================================
# Makefile -- Fulfillment Center
# =============================================================================

CC = gcc
CFLAGS = -Wall -Wextra -pedantic
SRCS = common.c warehouse.c supplier.c order_helper.c manage_restock_helper.c
OBJS=$(SRCS:.c=.o)
TARGETS = warehouse supplier order_helper manage_restock_helper
SCRIPTS = bootstrap.sh order.sh manage.sh
ARGS ?= 2 3 3 10 2 inventory.csv

# Default target.
build: $(TARGETS)
	@chmod +x $(SCRIPTS) 2>/dev/null || true

# --- Link: each executable = its own .o + common.o ---
warehouse: warehouse.o common.o
	$(CC) $(CFLAGS) -o $@ $^ -pthread

supplier: supplier.o common.o
	$(CC) $(CFLAGS) -o $@ $^

order_helper: order_helper.o common.o
	$(CC) $(CFLAGS) -o $@ $^

manage_restock_helper: manage_restock_helper.o common.o
	$(CC) $(CFLAGS) -o $@ $^

# --- Compile: each .o from its .c ---
%.o: %.c common.h
	$(CC) $(CFLAGS) -c $< -o $@

usage help:
	@echo "Fulfillment Center (Project 2026-3) - make rules"
	@echo ""
	@echo "  make build                compiles executables"
	@echo "  make run [ARGS=\"...\"]   launches the system through bootstrap.sh"
	@echo "  make clean                removes executables and IPC resources"
	@echo "  make usage                display this message"
	@echo ""
	@echo "Bootstrap interface (arguments passed via ARGS=\"...\"):"
	@echo "  <num_receivers> <num_pickers> <num_packers> <queue_capacity> <num_suppliers> <inventory.csv>"
	@echo ""
	@echo "Example:"
	@echo "  make run ARGS=\"15 20 30 15 10 inventory.csv\""
	@echo ""
	@echo "Default (make run without ARGS):  $(ARGS)"
	@echo ""
	@echo "Other commands in the project:"
	@echo "  ./order.sh  <client_id> <item_id> <quantity>"
	@echo "  ./manage.sh status | restock <item_id> <qty> | report | shutdown"
# --- clean ---
clean:
	rm -f $(OBJS) $(TARGETS)
	rm -f /tmp/orders_fifo /tmp/restock_fifo /tmp/order_resp_* /tmp/wh_status.tmp /tmp/warehouse.pid /tmp/suppliers.pid
	rm -f orders.log
	rm -rf supplier_configs

# --- run: example scenario "2 3 3 10 2 inventory.csv" ---
run: build
	./bootstrap.sh $(ARGS)