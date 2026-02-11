# Project 2: Index Freshness + Incremental Updates (CDC + Replay + Lag Monitoring)

This repo demonstrates a production-style pattern:
- Source DB (truth) + CDC event log with monotonically increasing offsets
- Incremental indexer that consumes CDC and updates an index idempotently
- Replay/backfill tooling
- Freshness monitoring (lag) + health API

## Use Case
Real-life use case

A customer support agent creates a new Case or updates a ticket, and the AI agent needs to “see” it immediately.

Why it matters

Without incremental updates, AI answers are stale

Full rebuilds are too slow + expensive

Enables real-time use cases like:

“Summarize what happened in the last 10 minutes”

“Find similar incidents right now”

## 1) Setup
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

## 2) Seed demo data
python -m app.seed

## 3) Run the indexer (incremental consumer)
python -m app.indexer

## 4) Run the health API (optional)
uvicorn app.api:app --reload

Then visit:
- http://127.0.0.1:8000/health
- http://127.0.0.1:8000/metrics

## 5) Create updates (emit CDC)
python -m app.cdc demo-updates

## 6) Backfill / rebuild index
python -m app.backfill rebuild

## 7) Run tests
pytest -q
