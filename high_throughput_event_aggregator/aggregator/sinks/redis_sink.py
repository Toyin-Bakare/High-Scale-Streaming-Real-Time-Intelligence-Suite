from __future__ import annotations
from typing import Dict, Any, List
from datetime import datetime
import redis
import json
from aggregator.windowing import dims_hash

class RedisSink:
    def __init__(self, url: str, ttl_seconds: int = 600):
        self.r = redis.Redis.from_url(url, decode_responses=True)
        self.ttl_seconds = ttl_seconds

    def write_rollups(self, metric: str, window_seconds: int, window_start: datetime, rollups: List[Dict[str, Any]]) -> None:
        if not rollups:
            return
        ws = window_start.isoformat()
        latest_key = f"hta:latest:{metric}:{window_seconds}"
        window_key = f"hta:window:{metric}:{ws}:{window_seconds}"

        pipe = self.r.pipeline(transaction=False)
        for r in rollups:
            dh = dims_hash(r["dims"])
            payload = {
                "window_start": r["window_start"].isoformat(),
                "window_end": r["window_end"].isoformat(),
                "metric": r["metric"],
                "dims": r["dims"],
                "count": int(r["count"]),
                "sum": float(r["sum"]),
                "uniques": int(r["uniques"]),
            }
            pipe.hset(latest_key, dh, json.dumps(payload, sort_keys=True))
            pipe.hset(window_key, dh, json.dumps(payload, sort_keys=True))
        pipe.expire(latest_key, self.ttl_seconds)
        pipe.expire(window_key, self.ttl_seconds)
        pipe.execute()

    def get_latest(self, metric: str, window_seconds: int) -> List[Dict[str, Any]]:
        key = f"hta:latest:{metric}:{window_seconds}"
        d = self.r.hgetall(key)
        return [json.loads(v) for v in d.values()]
