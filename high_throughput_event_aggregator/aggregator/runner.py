from __future__ import annotations
import asyncio, time
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional
from aiokafka import AIOKafkaConsumer
import orjson

from aggregator.config import Settings
from aggregator.schemas import Event
from aggregator.windowing import window_bounds, normalize_dims
from aggregator.state import WindowState
from aggregator.sinks.postgres_sink import PostgresSink
from aggregator.sinks.redis_sink import RedisSink

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

class AggregatorRunner:
    def __init__(self, settings: Settings):
        self.s = settings
        self.state = WindowState(retention_windows=30)
        self.pg = PostgresSink(self.s.postgres_url)
        self.redis = RedisSink(self.s.redis_url, ttl_seconds=self.s.redis_ttl_seconds)

        self._consumer: Optional[AIOKafkaConsumer] = None
        self._last_flush = time.time()
        self._buffered = 0

    async def start(self):
        self._consumer = AIOKafkaConsumer(
            self.s.topic,
            bootstrap_servers=self.s.kafka_bootstrap,
            group_id=self.s.group,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            max_poll_records=2000,
        )
        await self._consumer.start()

    async def stop(self):
        if self._consumer:
            await self._consumer.stop()
            self._consumer = None

    async def run_forever(self):
        assert self._consumer is not None
        try:
            async for msg in self._consumer:
                await self._handle_message(msg.value)
                self._buffered += 1
                if (time.time() - self._last_flush) >= self.s.flush_interval_seconds or self._buffered >= self.s.max_buffer_events:
                    await self.flush_and_commit()
        finally:
            await self.stop()

    async def _handle_message(self, raw: bytes):
        try:
            evt = Event.model_validate(orjson.loads(raw))
        except Exception:
            return

        wstart, wend = window_bounds(evt.ts, self.s.window_seconds)
        dims = normalize_dims({
            "customer_id": evt.customer_id,
            "endpoint": evt.endpoint,
            "status": evt.status,
            "event_type": evt.event_type,
        })
        self.state.update(metric=evt.metric, window_start=wstart, window_end=wend, dims=dims, value=evt.value, user_id=evt.user_id)

    async def flush_and_commit(self):
        assert self._consumer is not None
        now = _now_utc()
        ready = self.state.snapshot_ready(now=now, flush_lag_seconds=0)
        if not ready:
            self._last_flush = time.time()
            self._buffered = 0
            return

        grouped: Dict[tuple, List[Dict[str, Any]]] = {}
        for metric, wstart, wend, dims, cell in ready:
            grouped.setdefault((metric, wstart), []).append({
                "metric": metric,
                "window_start": wstart,
                "window_end": wend,
                "dims": dims,
                "count": cell.count,
                "sum": cell.sum,
                "uniques": len(cell.unique_users),
                "updated_at": now,
            })

        for (metric, wstart), rollups in grouped.items():
            self.pg.upsert_rollups(rollups)
            self.redis.write_rollups(metric=metric, window_seconds=self.s.window_seconds, window_start=wstart, rollups=rollups)

        keep_after = now - timedelta(seconds=self.s.window_seconds * 5)
        self.state.evict_older_than(keep_after=keep_after)

        await self._consumer.commit()
        self._last_flush = time.time()
        self._buffered = 0
