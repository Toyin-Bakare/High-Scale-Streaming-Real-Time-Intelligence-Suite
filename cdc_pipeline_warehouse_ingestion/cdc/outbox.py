from __future__ import annotations
from typing import Dict, Any
import hashlib, json
from sqlalchemy.orm import Session
from cdc.models import Order, OutboxEvent

def _idem_key(event_type: str, aggregate_id: str, payload: Dict[str, Any]) -> str:
    raw = json.dumps({"t": event_type, "id": aggregate_id, "p": payload}, sort_keys=True).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()

def create_order(db: Session, order_id: str, customer_id: str, amount_kobo: int, currency: str = "NGN") -> Order:
    order = Order(order_id=order_id, customer_id=customer_id, amount_kobo=amount_kobo, currency=currency, status="CREATED")
    db.add(order)
    db.flush()

    payload = {
        "order_id": order.order_id,
        "customer_id": order.customer_id,
        "amount_kobo": int(order.amount_kobo),
        "currency": order.currency,
        "status": order.status,
    }
    evt = OutboxEvent(
        aggregate_type="order",
        aggregate_id=order.order_id,
        event_type="order.created",
        payload=payload,
        idempotency_key=_idem_key("order.created", order.order_id, payload),
    )
    db.add(evt)
    db.commit()
    return order

def update_order(db: Session, order_id: str, patch: Dict[str, Any]) -> Order:
    order = db.get(Order, order_id)
    if not order:
        order = Order(order_id=order_id, customer_id=patch.get("customer_id","unknown"), amount_kobo=int(patch.get("amount_kobo",0)),
                      currency=patch.get("currency","NGN"), status=patch.get("status","CREATED"))
        db.add(order)
        db.flush()

    for k, v in patch.items():
        if hasattr(order, k):
            setattr(order, k, v)

    db.flush()

    payload = {
        "order_id": order.order_id,
        "customer_id": order.customer_id,
        "amount_kobo": int(order.amount_kobo),
        "currency": order.currency,
        "status": order.status,
        "updated_at": order.updated_at.isoformat() if order.updated_at else None,
    }
    evt = OutboxEvent(
        aggregate_type="order",
        aggregate_id=order.order_id,
        event_type="order.updated",
        payload=payload,
        idempotency_key=_idem_key("order.updated", order.order_id, payload),
    )
    db.add(evt)
    db.commit()
    return order
