import time
import sys
from app.db import connect, init_db

def upsert_source_and_emit(dataset: str, record_id: str, payload: str):
    """
    Writes to source_records and emits a CDC upsert event.
    In real life CDC could be Debezium, DB logs, or app-level outbox.
    """
    now = int(time.time())
    with connect() as conn:
        cur = conn.execute(
            "SELECT version FROM source_records WHERE dataset=? AND record_id=?",
            (dataset, record_id)
        )
        row = cur.fetchone()
        version = (row[0] + 1) if row else 1

        conn.execute(
            """
            INSERT INTO source_records(dataset, record_id, payload, version, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(dataset, record_id) DO UPDATE SET
              payload=excluded.payload,
              version=excluded.version,
              updated_at=excluded.updated_at
            """,
            (dataset, record_id, payload, version, now)
        )

        conn.execute(
            """
            INSERT INTO cdc_events(dataset, record_id, op, payload, version, event_ts)
            VALUES (?, ?, 'upsert', ?, ?, ?)
            """,
            (dataset, record_id, payload, version, now)
        )
        conn.commit()

def delete_source_and_emit(dataset: str, record_id: str):
    now = int(time.time())
    with connect() as conn:
        cur = conn.execute(
            "SELECT version FROM source_records WHERE dataset=? AND record_id=?",
            (dataset, record_id)
        )
        row = cur.fetchone()
        version = (row[0] + 1) if row else 1

        conn.execute(
            "DELETE FROM source_records WHERE dataset=? AND record_id=?",
            (dataset, record_id)
        )
        conn.execute(
            """
            INSERT INTO cdc_events(dataset, record_id, op, payload, version, event_ts)
            VALUES (?, ?, 'delete', NULL, ?, ?)
            """,
            (dataset, record_id, version, now)
        )
        conn.commit()

def demo_updates():
    upsert_source_and_emit("cases", "CASE-100", "Customer cannot login; reset attempted")
    upsert_source_and_emit("cases", "CASE-101", "Billing issue; refund requested")
    upsert_source_and_emit("kb", "KB-200", "How to reset MFA for admin users")
    time.sleep(1)
    upsert_source_and_emit("cases", "CASE-100", "Customer cannot login; root cause: expired token")
    delete_source_and_emit("kb", "KB-200")

if __name__ == "__main__":
    init_db()
    if len(sys.argv) >= 2 and sys.argv[1] == "demo-updates":
        demo_updates()
        print("Demo updates emitted to CDC.")
    else:
        print("Usage: python -m app.cdc demo-updates")
