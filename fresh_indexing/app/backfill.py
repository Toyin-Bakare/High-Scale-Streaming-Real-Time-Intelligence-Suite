import time
import sys
from app.db import connect, init_db

def rebuild_index(dataset: str):
    now = int(time.time())
    with connect() as conn:
        conn.execute("DELETE FROM index_records WHERE dataset=?", (dataset,))

        cur = conn.execute(
            "SELECT record_id, payload, version FROM source_records WHERE dataset=?",
            (dataset,)
        )
        rows = cur.fetchall()
        for record_id, payload, version in rows:
            conn.execute(
                """
                INSERT INTO index_records(dataset, record_id, payload, version, indexed_at, tombstone)
                VALUES (?, ?, ?, ?, ?, 0)
                """,
                (dataset, record_id, payload, int(version), now)
            )

        conn.execute(
            """
            INSERT INTO consumer_checkpoint(dataset, last_offset, last_event_ts, updated_at)
            VALUES (?, 0, 0, ?)
            ON CONFLICT(dataset) DO UPDATE SET
              last_offset=0, last_event_ts=0, updated_at=excluded.updated_at
            """,
            (dataset, now)
        )
        conn.commit()

def replay_from_offset(dataset: str, offset: int):
    now = int(time.time())
    with connect() as conn:
        conn.execute(
            """
            INSERT INTO consumer_checkpoint(dataset, last_offset, last_event_ts, updated_at)
            VALUES (?, ?, 0, ?)
            ON CONFLICT(dataset) DO UPDATE SET
              last_offset=excluded.last_offset,
              last_event_ts=0,
              updated_at=excluded.updated_at
            """,
            (dataset, offset, now)
        )
        conn.commit()

if __name__ == "__main__":
    init_db()
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python -m app.backfill rebuild [dataset]")
        print("  python -m app.backfill replay [dataset] [offset]")
        raise SystemExit(1)

    cmd = sys.argv[1]
    if cmd == "rebuild":
        dataset = sys.argv[2] if len(sys.argv) >= 3 else "cases"
        rebuild_index(dataset)
        print(f"Rebuilt index for dataset={dataset}. Checkpoint reset.")
    elif cmd == "replay":
        dataset = sys.argv[2] if len(sys.argv) >= 3 else "cases"
        offset = int(sys.argv[3]) if len(sys.argv) >= 4 else 0
        replay_from_offset(dataset, offset)
        print(f"Set checkpoint for dataset={dataset} to offset={offset}. Now run the indexer to replay.")
    else:
        print(f"Unknown command: {cmd}")
        raise SystemExit(1)
