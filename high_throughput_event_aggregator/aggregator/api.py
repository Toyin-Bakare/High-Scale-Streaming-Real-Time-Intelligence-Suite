from __future__ import annotations
from datetime import datetime, timezone
from fastapi import FastAPI, Query
from aggregator.config import settings
from aggregator.sinks.redis_sink import RedisSink
from aggregator.sinks.postgres_sink import PostgresSink

app = FastAPI(title="High-Throughput Event Aggregator API", version="1.0.0")

redis_sink = RedisSink(settings.redis_url, ttl_seconds=settings.redis_ttl_seconds)
pg_sink = PostgresSink(settings.postgres_url)

def _parse_dt(s: str) -> datetime:
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt

@app.get("/v1/latest")
def latest(metric: str = Query(...), window_seconds: int = Query(60)):
    return {"metric": metric, "window_seconds": window_seconds, "rollups": redis_sink.get_latest(metric, window_seconds)}

@app.get("/v1/history")
def history(
    metric: str = Query(...),
    from_ts: str = Query(...),
    to_ts: str = Query(...),
    window_seconds: int = Query(60),
    limit: int = Query(200, ge=1, le=2000),
):
    rows = pg_sink.query_history(metric=metric, from_ts=_parse_dt(from_ts), to_ts=_parse_dt(to_ts), limit=limit)
    return {"metric": metric, "window_seconds": window_seconds, "rows": rows}
