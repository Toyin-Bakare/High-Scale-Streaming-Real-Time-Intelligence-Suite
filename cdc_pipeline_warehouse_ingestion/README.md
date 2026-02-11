# CDC Pipeline for Warehouse Ingestion (Outbox → Kafka → DuckDB/Parquet)

A portfolio-ready reference implementation of a **Change Data Capture (CDC)** style ingestion pipeline that moves
operational data into an analytics warehouse with **replay safety**, **idempotent upserts**, and **observable checkpoints**.

This project uses the **Outbox Pattern** (Postgres table) to avoid the operational complexity of Debezium/Kafka Connect
while still demonstrating the architecture patterns companies use to build reliable ingestion systems.

---

## Problem Statement

Product systems write data into an operational database (OLTP) like Postgres. Analytics, ML features, and customer-facing
AI agents require that same data to be available in a warehouse / lake with:

- **Low lag** (minutes)
- **Correctness** (no missing events, no double-counting)
- **Idempotency** (safe retries & replays)
- **Backfill & replay** (reprocess from a checkpoint)
- **Schema evolution tolerance** (new fields shouldn’t break ingestion)
- **Operational visibility** (offsets/checkpoints, audit log)

### Common real-world failures this project addresses
- A service updates an order, but the warehouse never reflects the change (missed CDC event)
- A publisher retries and emits duplicates → warehouse double-counts revenue
- A consumer crashes mid-batch and replays messages → need idempotent merge
- Events arrive out of order (update before create) → require upsert semantics
- New fields get added in payload → pipeline must not crash

---

## How this repo solves the problem

### High-level flow
1) **Postgres** stores operational table `orders` and an **outbox** table `outbox_events`.
2) **Publisher** polls unpublished outbox rows and publishes envelopes to Kafka topic `cdc.orders`.
3) **Consumer** reads Kafka events and:
   - appends raw envelopes to **Parquet** (immutable lake/audit layer)
   - merges into **DuckDB** (`wh_orders`) using **idempotent upsert**
   - tracks consumer progress in `wh_checkpoints`

### Why this is reliable
- **Outbox** guarantees “write data + emit event” happen in the same DB transaction boundary.
- **Idempotency key** prevents duplicates from corrupting aggregates.
- **UPSERT** makes late/out-of-order updates safe.
- **Raw Parquet lake** provides an audit trail and enables replay/backfills.

---

## Repository Structure (How each file solves the problem)

### Infra
- **`docker-compose.yml`** — starts Postgres + Kafka locally for an end-to-end demo.
- **`requirements.txt`** — Python runtime deps: SQLAlchemy, Kafka client, DuckDB, PyArrow.

### Source & CDC Producer (`cdc/`)
- **`cdc/config.py`** — central config (DB URL, Kafka bootstrap, topic names, warehouse paths).
- **`cdc/db.py`** — Postgres engine/session + `init_db()` to create demo tables.
- **`cdc/models.py`** — SQLAlchemy models:
  - `orders` (source table)
  - `outbox_events` (CDC event buffer)
- **`cdc/outbox.py`** — outbox helpers:
  - `create_order()` writes to `orders` and emits `order.created`
  - `update_order()` updates `orders` and emits `order.updated`
  - generates a stable **idempotency key** for safe retries
- **`cdc/publisher.py`** — outbox publisher:
  - polls `outbox_events` where `published_at IS NULL`
  - publishes to Kafka with metadata headers
  - marks events published after successful produce

### Warehouse & Consumer (`cdc/warehouse/`)
- **`cdc/warehouse/duckdb_store.py`**
  - Initializes DuckDB tables:
    - `wh_orders` (serving/warehouse table)
    - `wh_applied_events` (idempotency guard)
    - `wh_checkpoints` (topic/partition offsets)
  - Applies events via **UPSERT** and records idempotency + checkpoints
- **`cdc/warehouse/parquet_sink.py`**
  - Writes raw CDC envelopes to partitioned Parquet:
    - `warehouse/lake/orders/date=YYYY-MM-DD/*.parquet`
- **`cdc/consumer.py`**
  - Kafka consumer loop:
    - parse/validate envelope
    - write to Parquet (audit)
    - apply to DuckDB (warehouse)
    - commit Kafka offset only after durable writes

### CLI Demo
- **`cdc/cli.py`**
  - `seed` — creates orders and emits outbox events
  - `update` — updates an order and emits an outbox event
  - `publish` — publishes outbox → Kafka
  - `consume` — ingests Kafka → DuckDB/Parquet
  - `query` — prints warehouse state from DuckDB

### Tests
- **`tests/test_idempotent_upsert.py`**
  - Verifies reprocessing the same event twice does not create duplicates.

---

## Quickstart

### 1) Start infra
```bash
docker compose up -d
```

### 2) Create topic
```bash
docker exec -it $(docker ps -qf name=cdc_kafka) bash -lc \
  "kafka-topics --create --topic cdc.orders --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 || true"
```

### 3) Install deps
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### 4) Seed Postgres + outbox
```bash
python -m cdc.cli seed --count 25
```

### 5) Publish outbox to Kafka
```bash
python -m cdc.cli publish --batch-size 200
```

### 6) Consume into DuckDB + Parquet
```bash
python -m cdc.cli consume --max-messages 200
```

### 7) Query warehouse
```bash
python -m cdc.cli query --limit 10
```

---

## Outputs
- DuckDB warehouse: `warehouse/warehouse.duckdb`
- Raw lake: `warehouse/lake/orders/date=YYYY-MM-DD/*.parquet`

---

## Resume-ready highlights
- Built CDC ingestion pipeline using Outbox + Kafka
- Implemented replay-safe consumer with idempotent upsert and checkpoints
- Produced raw Parquet audit log + serving warehouse table
- Designed for schema tolerance and operational reliability
