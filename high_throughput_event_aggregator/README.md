# High-Throughput Event Aggregator (Kafka → Windowed Aggregations → Redis/Postgres + API)

A portfolio-ready reference implementation of a **high-throughput event aggregation service** that ingests raw events
from Kafka and produces **low-latency rollups** (counters, sums, uniques) suitable for:

- real-time dashboards (support ops, usage analytics, billing)
- feature store / ML signals (near-real-time behavioral features)
- alerting (spikes, drops, SLO violation indicators)
- AI grounding signals (“what just changed?”)

This project focuses on **throughput, correctness under retries, windowing**, and **operational visibility**.

---

## Problem Statement

Modern systems emit large volumes of events (clicks, ticket updates, payment attempts, API calls). Downstream users rarely
need raw events; they need **aggregates**:

- “requests per minute by customer”
- “error rate by endpoint”
- “orders by status by region”
- “unique users active in the last 5 minutes”

### Common real-world failures this project addresses

- **Hot keys / skew**: one customer or endpoint dominates traffic, causing lag
- **Consumer restarts**: messages replay, causing double-counted aggregates unless idempotent
- **Late/out-of-order events**: event time != ingestion time; windows must be handled carefully
- **Backpressure**: slow sinks (DB) reduce throughput unless you batch and decouple
- **Operational blind spots**: without metrics, you don’t know lag, throughput, flush latency, or error rates

---

## How this repo solves the problem

### High-level flow
1) Producers write `events.raw` into Kafka.
2) The aggregator consumes events with a consumer group:
   - validates schema
   - updates **in-memory windowed state** (tumbling windows, e.g., 60s)
   - flushes aggregates in batches to:
     - **Redis** (fast reads / dashboards)
     - **Postgres** (durable history / audit)
   - commits offsets only after successful flush for “at-least-once + idempotent sink” safety.

### Key reliability/throughput ideas implemented
- **Windowed state** keyed by `(metric, dimensions, window_start)`
- **Batching** to Redis/Postgres to reduce per-event overhead
- **Idempotent writes** to Postgres using `(window_start, window_end, metric, dims_hash)` as a natural key
- **Offset commits after durable flush** to avoid counting events that didn’t persist
- **Configurable flush interval** and max buffer size for backpressure control

---

## Repository Structure (How each file solves the problem)

### Infra
- **`docker-compose.yml`**
  - Runs Kafka, Redis, Postgres locally for an end-to-end demo.
- **`requirements.txt`**
  - Python deps: aiokafka, FastAPI, SQLAlchemy, Redis client, etc.

### Core aggregator (`aggregator/`)
- **`aggregator/config.py`** — central config (Kafka/DB/Redis URLs, window size, flush interval).
- **`aggregator/schemas.py`** — Pydantic models for events + rollups (input validation).
- **`aggregator/windowing.py`** — tumbling window math + dimension normalization + stable dims hashing.
- **`aggregator/state.py`** — in-memory windowed aggregation state (count/sum/uniques, eviction).
- **`aggregator/sinks/postgres_sink.py`** — durable sink with **idempotent UPSERT** for rollups.
- **`aggregator/sinks/redis_sink.py`** — low-latency sink with TTL’d rollups for dashboards.
- **`aggregator/runner.py`** — Kafka consumer loop (batch, flush, commit).
- **`aggregator/producer.py`** — synthetic high-rate producer (hot-key probability to simulate skew).
- **`aggregator/api.py`** — FastAPI read API (latest via Redis, history via Postgres).
- **`aggregator/cli.py`** — one CLI: `produce`, `run`, `api`.

### Tests
- **`tests/test_windowing.py`** — validates window boundaries.
- **`tests/test_state.py`** — validates aggregation correctness and uniqueness counting.

---

## Quickstart

### 1) Start infrastructure
```bash
docker compose up -d
```

### 2) Create Kafka topic (optional if auto-create enabled)
```bash
docker exec -it $(docker ps -qf name=hta_kafka) bash -lc \
  "kafka-topics --create --topic events.raw --bootstrap-server localhost:9092 --partitions 6 --replication-factor 1 || true"
```

### 3) Install deps
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### 4) Start the aggregator
```bash
python -m aggregator.cli run --topic events.raw --group hta --window-seconds 60
```

### 5) Produce load
```bash
python -m aggregator.cli produce --topic events.raw --rate 200 --seconds 30 --hot-key-prob 0.20
```

### 6) Start API
```bash
python -m aggregator.cli api --port 8000
```

Open docs:
- http://127.0.0.1:8000/docs
