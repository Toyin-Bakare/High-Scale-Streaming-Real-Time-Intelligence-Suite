import uuid
from sqlalchemy import Column, String, DateTime, BigInteger, Integer, JSON, Index
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from cdc.db import Base

class Order(Base):
    __tablename__ = "orders"
    order_id = Column(String(64), primary_key=True)
    customer_id = Column(String(64), nullable=False)
    amount_kobo = Column(BigInteger, nullable=False)
    currency = Column(String(8), nullable=False, default="NGN")
    status = Column(String(20), nullable=False, default="CREATED")
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

class OutboxEvent(Base):
    __tablename__ = "outbox_events"
    event_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    aggregate_type = Column(String(40), nullable=False)
    aggregate_id = Column(String(64), nullable=False)
    event_type = Column(String(40), nullable=False)
    payload = Column(JSON, nullable=False)
    idempotency_key = Column(String(120), nullable=False)
    published_at = Column(DateTime(timezone=True), nullable=True)
    publish_attempts = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        Index("idx_outbox_unpublished", "published_at", "created_at"),
        Index("idx_outbox_aggregate", "aggregate_type", "aggregate_id"),
        Index("idx_outbox_idem", "idempotency_key"),
    )
