# High-Scale-Streaming-Real-Time-Intelligence-Suite

Advanced patterns for Change Data Capture (CDC) and event aggregation. Solves the challenge of maintaining index freshness and feature store consistency in high-throughput messaging environments

## Projects

### 1. Member Messaging Feature Store
**Tech:** Apache Flink, Kafka (MSK-compatible), Java, Docker  
A real-time feature store that merges recent user activity and messaging history to generate unified per-member messaging features for eligibility, suppression, dashboards, and experimentation.


📁 [`member-messaging-feature-store`](https://github.com/Toyin-Bakare/High-Scale-Streaming-Real-Time-Intelligence-Suite/tree/main/Member%20Messaging%20Feature%20Store)

---

### 2. High-Throughput Event Aggregator 
**Tech:** Kafka & Kotlin  
Service that consumes a high-volume stream of "transaction" events from Kafka and aggregates them into time-series windows (e.g., total spend per category every minute).

Focus: Implement "exactly-once" processing semantics and handle late-arriving data.

Use Case: Kafka and the real-time data processing that can be used for Risk and Applied AI.

📁 [`High-Throughput-Event-Aggregator`](https://github.com/Toyin-Bakare/High-Scale-Streaming-Real-Time-Intelligence-Suite/tree/main/high_throughput_event_aggregator)

---
### 3. Database-to-Data-Warehouse CDC Pipeline 
**Tech:** Python & Spark  
A Change Data Capture (CDC) pipeline that monitors a relational database (PostgreSQL) and incrementally syncs changes to a simulated Data Lake (Parquet files) or Snowflake.

Focus: Schema evolution and data integrity checks.

Use Case : Data Integrations and Transformations

📁 [`Database-to-Data-Warehouse`](https://github.com/Toyin-Bakare/High-Scale-Streaming-Real-Time-Intelligence-Suite/tree/main/cdc_pipeline_warehouse_ingestion)

---


### 4. Index Freshness + Incremental Updates (CDC + Replay + Lag Monitoring)
This repo demonstrates a production-style pattern: Source DB (truth) + CDC event log with monotonically increasing offsets;
Incremental indexer that consumes CDC and updates an index idempotently; Replay/backfill tooling; Freshness monitoring (lag) + health API

Focus: Real Time updates, reduces potential of stale updates

Use Case: A customer support agent creates a new Case or updates a ticket, and the AI agent needs to “see” it immediately.
Enables real-time use cases like: “Summarize what happened in the last 10 minutes” or “Find similar incidents right now”

Why it matters: Without incremental updates, AI answers are stale; Full rebuilds are too slow + expensive

📁 [`Index Freshness + Incremental Updates`](https://github.com/Toyin-Bakare/High-Scale-Streaming-Real-Time-Intelligence-Suite/tree/main/fresh_indexing)





=============
## Notes
- All projects are built as portfolio examples and do not include proprietary code.
- Where applicable, projects include local Docker setups for reproducibility.

---

## Contact
- LinkedIn: https://www.linkedin.com/in/toyinobakare
- Email: tonyobaker@gmail.com
