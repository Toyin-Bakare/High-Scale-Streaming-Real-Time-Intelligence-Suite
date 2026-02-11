from __future__ import annotations
import argparse, asyncio
import uvicorn
from aggregator.config import settings, Settings
from aggregator.runner import AggregatorRunner
from aggregator.producer import produce_events

def cmd_run(args):
    s = Settings(
        kafka_bootstrap=args.kafka_bootstrap,
        topic=args.topic,
        group=args.group,
        window_seconds=args.window_seconds,
        flush_interval_seconds=args.flush_interval_seconds,
        max_buffer_events=args.max_buffer_events,
        postgres_url=args.postgres_url,
        redis_url=args.redis_url,
        redis_ttl_seconds=args.redis_ttl_seconds,
    )

    async def _main():
        runner = AggregatorRunner(s)
        await runner.start()
        await runner.run_forever()

    asyncio.run(_main())

def cmd_produce(args):
    asyncio.run(produce_events(
        bootstrap=args.kafka_bootstrap,
        topic=args.topic,
        rate_per_sec=args.rate,
        seconds=args.seconds,
        hot_key_prob=args.hot_key_prob,
    ))

def cmd_api(args):
    uvicorn.run("aggregator.api:app", host="0.0.0.0", port=args.port, reload=False)

def main():
    p = argparse.ArgumentParser(prog="hta")
    sub = p.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("run")
    r.add_argument("--kafka-bootstrap", default=settings.kafka_bootstrap)
    r.add_argument("--topic", default=settings.topic)
    r.add_argument("--group", default=settings.group)
    r.add_argument("--window-seconds", type=int, default=settings.window_seconds)
    r.add_argument("--flush-interval-seconds", type=float, default=settings.flush_interval_seconds)
    r.add_argument("--max-buffer-events", type=int, default=settings.max_buffer_events)
    r.add_argument("--postgres-url", default=settings.postgres_url)
    r.add_argument("--redis-url", default=settings.redis_url)
    r.add_argument("--redis-ttl-seconds", type=int, default=settings.redis_ttl_seconds)
    r.set_defaults(fn=cmd_run)

    pr = sub.add_parser("produce")
    pr.add_argument("--kafka-bootstrap", default=settings.kafka_bootstrap)
    pr.add_argument("--topic", default=settings.topic)
    pr.add_argument("--rate", type=int, default=200)
    pr.add_argument("--seconds", type=int, default=30)
    pr.add_argument("--hot-key-prob", type=float, default=0.2)
    pr.set_defaults(fn=cmd_produce)

    a = sub.add_parser("api")
    a.add_argument("--port", type=int, default=8000)
    a.set_defaults(fn=cmd_api)

    args = p.parse_args()
    args.fn(args)

if __name__ == "__main__":
    main()
