from __future__ import annotations
from typing import Dict, Any
import json
from confluent_kafka import Consumer
from cdc.config import settings
from cdc.warehouse.duckdb_store import DuckDBWarehouse
from cdc.warehouse.parquet_sink import ParquetLakeSink

def consume_to_warehouse(topic: str | None = None, group: str = "cdc-warehouse-consumer", max_messages: int = 0) -> Dict[str, Any]:
    topic = topic or settings.TOPIC_ORDERS
    consumer = Consumer({
        "bootstrap.servers": settings.KAFKA_BOOTSTRAP,
        "group.id": group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([topic])

    wh = DuckDBWarehouse(settings.DUCKDB_PATH)
    lake = ParquetLakeSink(base_dir=settings.WAREHOUSE_DIR)

    processed = 0
    bad = 0

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                continue

            processed += 1
            try:
                envelope = json.loads(msg.value().decode("utf-8"))
            except Exception:
                bad += 1
                consumer.commit(message=msg, asynchronous=False)  # drop poison for demo
                if max_messages and processed >= max_messages:
                    break
                continue

            lake.append_orders_event(envelope)
            wh.apply_order_event(envelope, kafka_meta={
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
                "group": group,
            })

            consumer.commit(message=msg, asynchronous=False)

            if max_messages and processed >= max_messages:
                break
    finally:
        consumer.close()
        wh.close()

    return {"processed": processed, "bad": bad, "duckdb_path": settings.DUCKDB_PATH, "lake_dir": settings.WAREHOUSE_DIR}
