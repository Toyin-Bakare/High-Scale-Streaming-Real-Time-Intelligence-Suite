import time
from typing import Dict, Any
from app.db import connect
from app.config import settings

def get_dataset_metrics(dataset: str) -> Dict[str, Any]:
    now = int(time.time())
    with connect() as conn:
        cur = conn.execute(
            "SELECT MAX(event_ts), MAX(offset) FROM cdc_events WHERE dataset=?",
            (dataset,)
        )
        max_event_ts, max_offset = cur.fetchone()
        max_event_ts = int(max_event_ts) if max_event_ts is not None else 0
        max_offset = int(max_offset) if max_offset is not None else 0

        cur = conn.execute(
            "SELECT last_offset, last_event_ts, updated_at FROM consumer_checkpoint WHERE dataset=?",
            (dataset,)
        )
        row = cur.fetchone()
        if not row:
            last_offset, last_event_ts, updated_at = 0, 0, 0
        else:
            last_offset, last_event_ts, updated_at = int(row[0]), int(row[1]), int(row[2])

        freshness_lag_s = max(0, max_event_ts - last_event_ts) if max_event_ts and last_event_ts else (max_event_ts if max_event_ts else 0)
        backlog_offsets = max(0, max_offset - last_offset)

        ok = freshness_lag_s <= settings.FRESHNESS_SLO_SECONDS

        return {
            "dataset": dataset,
            "now": now,
            "max_event_ts": max_event_ts,
            "max_offset": max_offset,
            "checkpoint_last_event_ts": last_event_ts,
            "checkpoint_last_offset": last_offset,
            "checkpoint_updated_at": updated_at,
            "freshness_lag_seconds": freshness_lag_s,
            "backlog_offsets": backlog_offsets,
            "freshness_slo_seconds": settings.FRESHNESS_SLO_SECONDS,
            "ok": ok,
        }
