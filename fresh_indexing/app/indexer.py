import time
import sys
from typing import List, Dict, Tuple
from app.db import connect, init_db
from app.config import settings
from app.models import CDCEvent

def _get_checkpoint(conn, dataset: str) -> Tuple[int, int]:
    cur = conn.execute(
        "SELECT last_offset, last_event_ts FROM consumer_checkpoint WHERE dataset=?",
        (dataset,)
    )
    row = cur.fetchone()
    if not row:
        return (0, 0)
    return (int(row[0]), int(row[1]))

def _set_checkpoint(conn, dataset: str, last_offset: int, last_event_ts: int):
    now = int(time.time())
    conn.execute(
        """
        INSERT INTO consumer_checkpoint(dataset, last_offset, last_event_ts, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(dataset) DO UPDATE SET
          last_offset=excluded.last_offset,
          last_event_ts=excluded.last_event_ts,
          updated_at=excluded.updated_at
        """,
        (dataset, last_offset, last_event_ts, now)
    )

def _load_events(conn, dataset: str, after_offset: int, limit: int) -> List[CDCEvent]:
    cur = conn.execute(
        """
        SELECT offset, dataset, record_id, op, payload, version, event_ts
        FROM cdc_events
        WHERE dataset=? AND offset > ?
        ORDER BY offset ASC
        LIMIT ?
        """,
        (dataset, after_offset, limit)
    )
    rows = cur.fetchall()
    return [
        CDCEvent(
            offset=r[0], dataset=r[1], record_id=r[2], op=r[3],
            payload=r[4], version=r[5], event_ts=r[6]
        ) for r in rows
    ]

def _apply_event_idempotent(conn, ev: CDCEvent):
    cur = conn.execute(
        "SELECT version FROM index_records WHERE dataset=? AND record_id=?",
        (ev.dataset, ev.record_id)
    )
    row = cur.fetchone()
    current_version = int(row[0]) if row else 0
    if ev.version <= current_version:
        return

    now = int(time.time())
    if ev.op == "upsert":
        conn.execute(
            """
            INSERT INTO index_records(dataset, record_id, payload, version, indexed_at, tombstone)
            VALUES (?, ?, ?, ?, ?, 0)
            ON CONFLICT(dataset, record_id) DO UPDATE SET
              payload=excluded.payload,
              version=excluded.version,
              indexed_at=excluded.indexed_at,
              tombstone=0
            """,
            (ev.dataset, ev.record_id, ev.payload or "", ev.version, now)
        )
    else:
        conn.execute(
            """
            INSERT INTO index_records(dataset, record_id, payload, version, indexed_at, tombstone)
            VALUES (?, ?, NULL, ?, ?, 1)
            ON CONFLICT(dataset, record_id) DO UPDATE SET
              payload=NULL,
              version=excluded.version,
              indexed_at=excluded.indexed_at,
              tombstone=1
            """,
            (ev.dataset, ev.record_id, ev.version, now)
        )

def run_once(datasets: List[str]) -> Dict[str, int]:
    processed = {}
    with connect() as conn:
        for ds in datasets:
            last_offset, last_ts = _get_checkpoint(conn, ds)
            events = _load_events(conn, ds, last_offset, settings.CONSUMER_BATCH_SIZE)

            if not events:
                processed[ds] = 0
                continue

            max_offset = last_offset
            max_ts = last_ts

            for ev in events:
                _apply_event_idempotent(conn, ev)
                max_offset = max(max_offset, ev.offset)
                max_ts = max(max_ts, ev.event_ts)

            _set_checkpoint(conn, ds, max_offset, max_ts)
            processed[ds] = len(events)

        conn.commit()
    return processed

def loop(datasets: List[str], poll_seconds: float = 1.0):
    print(f"Indexer started for datasets={datasets}. Poll={poll_seconds}s")
    while True:
        counts = run_once(datasets)
        if any(v > 0 for v in counts.values()):
            print("Processed:", counts)
        time.sleep(poll_seconds)

if __name__ == "__main__":
    init_db()
    datasets = ["cases", "kb"]
    if len(sys.argv) >= 2 and sys.argv[1] == "once":
        print(run_once(datasets))
    else:
        loop(datasets)
