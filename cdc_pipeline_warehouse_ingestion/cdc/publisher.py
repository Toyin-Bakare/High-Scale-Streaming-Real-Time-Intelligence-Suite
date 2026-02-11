from __future__ import annotations
from typing import List, Dict, Any
import json
from confluent_kafka import Producer
from sqlalchemy import select
from sqlalchemy.orm import Session
from cdc.models import OutboxEvent
from cdc.config import settings

def _headers(evt: OutboxEvent) -> List[tuple]:
    return [
        ("event_type", evt.event_type.encode("utf-8")),
        ("aggregate_type", evt.aggregate_type.encode("utf-8")),
        ("aggregate_id", evt.aggregate_id.encode("utf-8")),
        ("idempotency_key", evt.idempotency_key.encode("utf-8")),
    ]

def publish_outbox(db: Session, batch_size: int = 200, topic: str | None = None) -> Dict[str, Any]:
    topic = topic or settings.TOPIC_ORDERS
    producer = Producer({"bootstrap.servers": settings.KAFKA_BOOTSTRAP})

    q = (select(OutboxEvent)
         .where(OutboxEvent.published_at.is_(None))
         .order_by(OutboxEvent.created_at.asc())
         .limit(batch_size))
    rows = list(db.execute(q).scalars().all())
    if not rows:
        return {"published": 0}

    errors = 0

    def delivery(err, msg):
        nonlocal errors
        if err is not None:
            errors += 1

    for evt in rows:
        evt.publish_attempts += 1
        envelope = {
            "event_id": str(evt.event_id),
            "event_type": evt.event_type,
            "aggregate_type": evt.aggregate_type,
            "aggregate_id": evt.aggregate_id,
            "idempotency_key": evt.idempotency_key,
            "payload": evt.payload,
            "emitted_at": evt.created_at.isoformat(),
            "schema_version": 1,
        }
        producer.produce(
            topic=topic,
            key=evt.aggregate_id.encode("utf-8"),
            value=json.dumps(envelope).encode("utf-8"),
            headers=_headers(evt),
            callback=delivery,
        )
        producer.poll(0)

    producer.flush()

    if errors == 0:
        from sqlalchemy.sql import func
        for evt in rows:
            evt.published_at = func.now()
        db.commit()
    else:
        db.commit()

    return {"published": len(rows), "errors": errors}
