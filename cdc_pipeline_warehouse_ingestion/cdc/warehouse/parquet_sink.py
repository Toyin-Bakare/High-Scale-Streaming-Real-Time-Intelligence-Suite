from __future__ import annotations
from typing import Dict, Any
import os, json
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone

class ParquetLakeSink:
    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        self.lake_root = os.path.join(base_dir, "lake", "orders")

    def _partition_dir(self, dt: datetime) -> str:
        return os.path.join(self.lake_root, f"date={dt.strftime('%Y-%m-%d')}")

    def append_orders_event(self, envelope: Dict[str, Any]) -> str:
        now = datetime.now(timezone.utc)
        part_dir = self._partition_dir(now)
        os.makedirs(part_dir, exist_ok=True)

        filename = f"events_{now.strftime('%H%M%S_%f')}.parquet"
        path = os.path.join(part_dir, filename)

        row = {
            "ingested_at": now.isoformat(),
            "event_id": str(envelope.get("event_id")),
            "event_type": str(envelope.get("event_type")),
            "aggregate_id": str(envelope.get("aggregate_id")),
            "idempotency_key": str(envelope.get("idempotency_key")),
            "schema_version": int(envelope.get("schema_version", 1)),
            "payload_json": json.dumps(envelope.get("payload") or {}, sort_keys=True),
            "emitted_at": envelope.get("emitted_at"),
        }

        table = pa.Table.from_pylist([row])
        pq.write_table(table, path)
        return path
