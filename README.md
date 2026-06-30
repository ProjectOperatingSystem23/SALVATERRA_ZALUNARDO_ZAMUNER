# Fulfillment Center

> Operating Systems Course — Project 2026-3
> A multi-process, multi-threaded simulation of an e-commerce fulfillment center, written in **C** and **Bash**.

An automated warehouse (Amazon-style) emulated as a set of cooperating processes. A single multi-threaded **warehouse** process manages an inventory and processes customer orders through thread pools (Order Receivers, Picker Robots, Packers), while separate **supplier** processes periodically deliver restocking shipments. Bash scripts bootstrap the system, place orders, and manage it at runtime.

---

## Table of contents

- [Architecture](#architecture)
- [Components](#components)
- [Build & run](#build--run)
- [Usage](#usage)
- [Design highlights](#design-highlights)
- [IPC channels](#ipc-channels)
- [Error codes](#error-codes)
- [Project layout](#project-layout)
- [Requirements](#requirements)

---

## Architecture

The system is a single multi-threaded process (`warehouse`) that owns the inventory and two bounded queues, surrounded by satellite processes that talk to it via **FIFOs** and **signals**.

```
order.sh → order_helper ──ORDERS_FIFO──→ [Receivers] → pending → [Pickers] → packaging → [Packers] → orders.log
                                                                                              │
                                          warehouse ──/tmp/order_resp_<PID>──→ order_helper (reply)
supplier(s) / manage.sh ──RESTOCK_FIFO──→ [Restock thread] → inventory (increment)
```

- **warehouse** — one process with four thread types: pools of **Receivers**, **Pickers**, **Packers** (sizes set at startup) plus one **Restock** thread. They share the inventory and queues in memory.
- **supplier** — N independent C processes launched by the bootstrap script; each reads a config file and sends timed restock shipments.
- **order_helper / manage_restock_helper** — small C helpers that perform the binary IPC on behalf of the Bash scripts.
- **bootstrap.sh / order.sh / manage.sh** — Bash scripts that validate input, orchestrate startup, and control the running system.

### Order lifecycle

```
RECEIVED → PICKING → PACKING → SHIPPED
                 └──────────────→ PARTIAL   (ships min(stock, requested))
        └──────────────────────→ REJECTED   (missing item / out of stock / invalid qty)
```

The Receiver does a fast optimistic stock *peek* to reject obvious cases early; the **binding stock decision is made by the Picker** under the inventory mutex, which is where the "last unit" race is resolved.

---

## Components

| File | Role |
|------|------|
| `common.h` / `common.c` | Shared binary contract: IPC paths, error codes, the three wire-format structs, and shared helpers (`write_all`, `setup_handler`, `open_fifo_r_dw`, `read_line_from_fd`). |
| `warehouse.c` | The multi-threaded core: inventory, two bounded buffers, four thread pools, signal handling, ordered shutdown. |
| `supplier.c` | Periodic restocker; reads a `.conf` schedule and writes `RestockMsg`s on a countdown scheduler. |
| `order.sh` + `order_helper.c` | Client side: Bash validates input and checks liveness, the C helper does the request/response FIFO protocol. |
| `manage.sh` + `manage_restock_helper.c` | Control panel: `status`, `restock`, `report`, `shutdown`. |
| `bootstrap.sh` | Validates inventory, creates FIFOs, generates supplier configs, launches all processes. |
| `makefile` | `build`, `clean`, `run` (+ `help`). |
| `inventory.csv` | Sample dataset: 40 items across 5 categories. |

---

## Build & run

Requires a Linux environment (tested on Ubuntu 24.04), `gcc`, `make`, and a POSIX shell.

```bash
# Compile the four executables (warehouse is linked with -pthread)
make build          # or simply: make

# Launch a scenario via bootstrap (default ARGS shown below)
make run            # = ./bootstrap.sh 2 3 3 10 2 inventory.csv

# Custom scenario
make run ARGS="15 20 30 15 10 inventory.csv"

# Remove executables, object files, IPC resources, logs and configs
make clean
```

`bootstrap.sh` arguments:

```
./bootstrap.sh <num_receivers> <num_pickers> <num_packers> <queue_capacity> <num_suppliers> <inventory.csv>
```

It launches the processes in the background and exits, leaving the system running.

---

## Usage

Once the system is running:

```bash
# Place an order: <client_id> <item_id> <quantity>
./order.sh Alice 101 2          # → [OK] shipped 2/2
./order.sh Bob 202 100          # → [PARTIAL] insufficient stock
./order.sh Carol 999 1          # → [REJECTED] item does not exist

# Manage the running system
./manage.sh status              # processes, queue sizes, inventory snapshot (via SIGUSR1)
./manage.sh restock 101 50      # manual restock of item 101 (+50 units) via IPC
./manage.sh report              # statistics from orders.log
./manage.sh shutdown            # graceful shutdown + IPC cleanup
```

### Quick demo

```bash
make build
./bootstrap.sh 2 3 3 10 2 inventory.csv
./order.sh Alice 101 2
./order.sh Bob 207 10            # item 207 has low stock → PARTIAL
./manage.sh restock 207 20
./manage.sh status
./manage.sh report
./manage.sh shutdown
```

---

## Design highlights

- **Producer/consumer monitors.** The Pending and Packaging queues are bounded circular buffers protected by a mutex and two condition variables (`not_full`, `not_empty`). Threads block in the kernel when the queue is full/empty — no busy-waiting or spinlocks.
- **Atomic last-unit handling.** Stock is decremented under a single inventory mutex with `ship = min(stock, requested)`, so concurrent pickers can never oversell; the difference is reported as a partial fill.
- **Atomic IPC messages.** All three message structs are fixed-size and well below `PIPE_BUF`, so each `write()` on a FIFO is atomic — multiple writers never interleave, and no length-prefixing or delimiters are needed.
- **Per-client reply channel.** Each client creates a private FIFO `/tmp/order_resp_<PID>` and carries its path inside the request, so the warehouse always replies to the right client.
- **Centralized signal handling.** Signals are blocked before threads are created and handled only by `main` via `sigsuspend`; handlers merely set a `volatile sig_atomic_t` flag (async-signal-safe).
- **Graceful, ordered shutdown.** On SIGTERM the pipeline is drained stage by stage (receivers → restock → pickers → packers) so in-flight orders complete before exit; IPC resources are then removed.
- **Robust I/O.** `write_all`/`read_all` handle short reads/writes and `EINTR`; the status dump is published with an atomic `rename` so readers never see a half-written file.
- **All-or-nothing startup.** `bootstrap.sh` uses an `EXIT` trap so a failed launch rolls back (kills started processes, removes created resources) and never leaves a half-started system.

---

## IPC channels

| Channel | Direction | Message |
|---------|-----------|---------|
| `/tmp/orders_fifo` | clients → warehouse | `OrderRequest` |
| `/tmp/restock_fifo` | suppliers / manual → warehouse | `RestockMsg` |
| `/tmp/order_resp_<PID>` | warehouse → client | `OrderResponse` |
| `SIGUSR1` | manage.sh → warehouse | status dump request |
| `SIGTERM` / `SIGINT` | manage.sh → processes | graceful shutdown |
| `SIGALRM` | client (internal) | response timeout |

A `RestockMsg` with `supplier_id = 0` denotes a **manual** restock; `supplier_id = -1` is the **stop sentinel** used internally at shutdown.

---

## Error codes

Defined once in `common.h` and mirrored with the same values in the Bash scripts, so outcomes cross the C/Bash boundary via the exit code (`$?`).

| Code | Value | Meaning | Code | Value | Meaning |
|------|-------|---------|------|-------|---------|
| `ERR_OK` | 0 | success | `ERR_PARTIAL_FILL` | 5 | partial shipment |
| `ERR_ITEM_NOT_FOUND` | 1 | item missing | `ERR_WAREHOUSE_DOWN` | 6 | warehouse not running |
| `ERR_OUT_OF_STOCK` | 2 | out of stock | `ERR_TIMEOUT` | 7 | IPC response timed out |
| `ERR_INVALID_QTY` | 3 | quantity ≤ 0 | `ERR_USAGE` | 8 | bad arguments |
| `ERR_IO` | 4 | I/O error | | | |

---

## Project layout

```
.
├── common.h                  # shared binary contract
├── common.c                  # shared helpers
├── warehouse.c               # multi-threaded core
├── supplier.c                # periodic restocker
├── order_helper.c            # client-side IPC helper
├── manage_restock_helper.c   # manual-restock IPC helper
├── bootstrap.sh              # environment setup + process launch
├── order.sh                  # place an order
├── manage.sh                 # status / restock / report / shutdown
├── inventory.csv             # sample inventory (40 items)
└── makefile                  # build / clean / run
```

Runtime artifacts (created while running): `orders.log`, `supplier_configs/`, and FIFOs / state files under `/tmp`.

---

## Requirements

- Linux (Ubuntu 24.04 recommended)
- `gcc` with `-pthread`, `make`
- A POSIX-compatible Bash shell

Scripts use relative paths (`orders.log`, `supplier_configs/`), so run all commands from the project directory.
