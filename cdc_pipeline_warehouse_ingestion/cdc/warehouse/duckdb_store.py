from __future__ import annotations
from typing import Dict, Any
import os
import duckdb

class DuckDBWarehouse:
    def __init__(self, db_path: str):
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        self.con = duckdb.connect(db_path)
        self._init_schema()

    def _init_schema(self) -> None:
        self.con.execute("""
        CREATE TABLE IF NOT EXISTS wh_orders (
          order_id VARCHAR PRIMARY KEY,
          customer_id VARCHAR,
          amount_kobo BIGINT,
          currency VARCHAR,
          status VARCHAR,
          last_event_type VARCHAR,
          last_event_id VARCHAR,
          last_idempotency_key VARCHAR,
          updated_at TIMESTAMP,
          created_at TIMESTAMP
        );
        """)
        self.con.execute("""
        CREATE TABLE IF NOT EXISTS wh_applied_events (
          idempotency_key VARCHAR PRIMARY KEY,
          event_id VARCHAR,
          applied_at TIMESTAMP DEFAULT NOW()
        );
        """)
        self.con.execute("""
        CREATE TABLE IF NOT EXISTS wh_checkpoints (
          topic VARCHAR,
          partition INTEGER,
          offset BIGINT,
          updated_at TIMESTAMP DEFAULT NOW(),
          PRIMARY KEY(topic, partition)
        );
        """)

    def apply_order_event(self, envelope: Dict[str, Any], kafka_meta: Dict[str, Any]) -> None:
        idem = str(envelope.get("idempotency_key") or "")
        event_id = str(envelope.get("event_id") or "")
        event_type = str(envelope.get("event_type") or "")
        payload = envelope.get("payload") or {}

        if not idem:
            raise ValueError("missing idempotency_key")

        already = self.con.execute("SELECT 1 FROM wh_applied_events WHERE idempotency_key = ? LIMIT 1", [idem]).fetchone()
        if already:
            self._checkpoint(kafka_meta)
            return

        order_id = str(payload.get("order_id") or "")
        if not order_id:
            raise ValueError("payload missing order_id")

        customer_id = payload.get("customer_id")
        amount_kobo = payload.get("amount_kobo")
        currency = payload.get("currency")
        status = payload.get("status")

        self.con.execute("""
        INSERT INTO wh_orders(order_id, customer_id, amount_kobo, currency, status, last_event_type, last_event_id, last_idempotency_key, updated_at, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
        ON CONFLICT(order_id) DO UPDATE SET
          customer_id = EXCLUDED.customer_id,
          amount_kobo = EXCLUDED.amount_kobo,
          currency = EXCLUDED.currency,
          status = EXCLUDED.status,
          last_event_type = EXCLUDED.last_event_type,
          last_event_id = EXCLUDED.last_event_id,
          last_idempotency_key = EXCLUDED.last_idempotency_key,
          updated_at = NOW();
        """, [order_id, customer_id, amount_kobo, currency, status, event_type, event_id, idem])

        self.con.execute("INSERT INTO wh_applied_events(idempotency_key, event_id) VALUES (?, ?)", [idem, event_id])
        self._checkpoint(kafka_meta)

    def _checkpoint(self, kafka_meta: Dict[str, Any]) -> None:
        topic = kafka_meta.get("topic")
        part = int(kafka_meta.get("partition"))
        off = int(kafka_meta.get("offset"))
        self.con.execute("""
        INSERT INTO wh_checkpoints(topic, partition, offset) VALUES (?, ?, ?)
        ON CONFLICT(topic, partition) DO UPDATE SET offset = EXCLUDED.offset, updated_at = NOW();
        """, [topic, part, off])

    def query_orders(self, limit: int = 20):
        return self.con.execute("SELECT * FROM wh_orders ORDER BY updated_at DESC LIMIT ?", [limit]).fetchdf()

    def close(self) -> None:
        try:
            self.con.close()
        except Exception:
            pass
