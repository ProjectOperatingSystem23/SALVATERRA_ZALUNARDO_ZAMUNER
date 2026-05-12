# Fulfillment Center — OS Lab Project 2026

> Automated e-commerce fulfillment center simulator.  
> Operating Systems Course — University of Trento, ICE Program  
> **Group:** Salvaterra · Zalunardo · Zamuner

---

## Overview

This project emulates an automated warehouse system inspired by services like Amazon.  
Multiple concurrent processes and threads cooperate to receive, process, and ship customer orders.

The system is composed of:
- A **Warehouse** process — heavily multi-threaded, manages inventory and order processing via thread pools
- Multiple **Supplier** processes — periodically restock inventory via IPC
- A set of **Bash scripts** — bootstrapping, order placement, and system management

---

## Architecture

```
order.sh ──[FIFO]──► Order Receivers (thread pool)
                              │
                     Pending Orders Queue  ◄── bounded buffer
                              │
                     Picker Robots (thread pool) ◄──► Inventory (shared memory)
                              │
                     Packaging Queue  ◄── bounded buffer
                              │
                     Packers (thread pool)
                              │
                          orders.log

supplier ──[FIFO]──► Warehouse (increments inventory)
```

---

## Project Structure

```
SALVATERRA_ZALUNARDO_ZAMUNER/
├── code/
│   ├── warehouse.c        # Main warehouse process
│   ├── supplier.c         # Supplier process
│   ├── bootstrap.sh       # System bootstrapping script
│   ├── order.sh           # Client order script
│   ├── manage.sh          # Management script
│   └── Makefile           # Build system
├── report.pdf             # Design report (max 5 pages)
└── README.md
```

---

## Build & Run

### Prerequisites
- Ubuntu 24.04
- GCC
- Make

### Compile
```bash
make build
```

### Run a demo scenario
```bash
make run
# or with custom arguments:
make run ARGS="--num-cooks=5"
```

### Clean
```bash
make clean
```

---

## Usage

### Bootstrap the system
```bash
./bootstrap.sh <num_receivers> <num_pickers> <num_packers> <queue_capacity> <num_suppliers> <inventory.csv>
```
Example:
```bash
./bootstrap.sh 3 4 2 10 2 inventory.csv
```

### Place an order
```bash
./order.sh <client_id> <item_id> <quantity>
```
Example:
```bash
./order.sh Alice 101 2
./order.sh Bob 205 1
```

### Manage the system
```bash
./manage.sh status              # Process status, queue sizes, inventory summary
./manage.sh restock 101 50      # Restock 50 units of item 101
./manage.sh report              # Order statistics from orders.log
./manage.sh shutdown            # Graceful shutdown + IPC cleanup
```

---

## Concurrency Design

| Resource | Synchronization |
|---|---|
| Inventory (shared memory) | Reader-Writer Lock (`pthread_rwlock_t`) |
| Pending Orders Queue | Mutex + Semaphores (blocking, no busy-wait) |
| Packaging Queue | Mutex + Semaphores (blocking, no busy-wait) |
| IPC (orders/restock) | Named FIFOs |

> Full justification of design choices in `report.pdf`.

---

## Error Codes

| Code | Meaning |
|---|---|
| `ITEM_NOT_FOUND` | Requested item does not exist in inventory |
| `OUT_OF_STOCK` | Item has zero units available |
| `INVALID_QUANTITY` | Quantity is zero or negative |
| `QUEUE_FULL` | Bounded buffer has reached capacity |

---

## Submission

Archive name: `Salvaterra_Zalunardo_Zamuner.tar.gz`  
Submitted via Moodle for the chosen exam attempt (June/July/September 2026 or January/February 2027).
