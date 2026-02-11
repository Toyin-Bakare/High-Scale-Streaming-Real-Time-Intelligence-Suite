from __future__ import annotations
import os
from pydantic import BaseModel

class Settings(BaseModel):
    kafka_bootstrap: str = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    topic: str = os.getenv("HTA_TOPIC", "events.raw")
    group: str = os.getenv("HTA_GROUP", "hta")

    window_seconds: int = int(os.getenv("HTA_WINDOW_SECONDS", "60"))
    flush_interval_seconds: float = float(os.getenv("HTA_FLUSH_INTERVAL_SECONDS", "2.0"))
    max_buffer_events: int = int(os.getenv("HTA_MAX_BUFFER_EVENTS", "50000"))

    postgres_url: str = os.getenv("POSTGRES_URL", "postgresql+psycopg2://hta:hta@localhost:5433/hta")
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    redis_ttl_seconds: int = int(os.getenv("HTA_REDIS_TTL_SECONDS", "600"))

settings = Settings()
