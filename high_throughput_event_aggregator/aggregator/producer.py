from __future__ import annotations
import asyncio, random, string, uuid
from datetime import datetime, timezone
from aiokafka import AIOKafkaProducer
import orjson

def _now():
    return datetime.now(timezone.utc)

def _rand_choice(seq):
    return seq[random.randint(0, len(seq)-1)]

def _rand_id(prefix: str, n: int = 8) -> str:
    return prefix + "-" + "".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(n))

async def produce_events(bootstrap: str, topic: str, rate_per_sec: int, seconds: int, hot_key_prob: float = 0.10):
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
    await producer.start()
    try:
        customers = [f"cust_{i:03d}" for i in range(1, 101)]
        endpoints = ["/api/search", "/api/orders", "/api/tickets", "/api/login", "/api/profile"]
        statuses = ["200", "400", "401", "404", "500"]
        metrics = ["requests", "errors", "latency_ms"]

        hot_customer = "cust_hot"
        hot_endpoint = "/api/search"

        total = rate_per_sec * seconds
        interval = 1.0 / max(1, rate_per_sec)

        for _ in range(total):
            ts = _now()
            is_hot = random.random() < hot_key_prob
            customer_id = hot_customer if is_hot else _rand_choice(customers)
            endpoint = hot_endpoint if is_hot else _rand_choice(endpoints)

            metric = _rand_choice(metrics)
            event_type = "request"
            status = _rand_choice(statuses)

            if metric == "errors":
                status = _rand_choice(["400","500","500","500","401"])
                value = 1.0
            elif metric == "latency_ms":
                value = float(random.randint(20, 1500))
            else:
                value = 1.0

            evt = {
                "event_id": str(uuid.uuid4()),
                "event_type": event_type,
                "metric": metric,
                "value": value,
                "user_id": _rand_id("u", 6),
                "customer_id": customer_id,
                "endpoint": endpoint,
                "status": status,
                "ts": ts.isoformat(),
                "extra": {"source": "synthetic"},
            }

            await producer.send_and_wait(topic, orjson.dumps(evt))
            await asyncio.sleep(interval)
    finally:
        await producer.stop()
