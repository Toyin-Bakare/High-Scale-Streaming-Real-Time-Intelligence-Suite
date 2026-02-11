from __future__ import annotations
import argparse, random, string
from sqlalchemy.orm import Session
from cdc.db import SessionLocal, init_db
from cdc import outbox
from cdc.publisher import publish_outbox
from cdc.consumer import consume_to_warehouse
from cdc.config import settings

def _rand_id(prefix: str, n: int = 6) -> str:
    return prefix + "-" + "".join(random.choice(string.digits) for _ in range(n))

def cmd_seed(args):
    init_db()
    db: Session = SessionLocal()
    try:
        for _ in range(args.count):
            oid = _rand_id("o")
            cid = _rand_id("c")
            amount = random.choice([50000, 120000, 350000, 999999, 0])
            outbox.create_order(db, oid, cid, amount, currency="NGN")
        print({"seeded_orders": args.count})
    finally:
        db.close()

def cmd_update(args):
    init_db()
    db: Session = SessionLocal()
    try:
        patch = {}
        if args.status:
            patch["status"] = args.status
        if args.amount_kobo is not None:
            patch["amount_kobo"] = int(args.amount_kobo)
        if args.customer_id:
            patch["customer_id"] = args.customer_id

        outbox.update_order(db, args.order_id, patch)
        print({"updated": args.order_id, "patch": patch})
    finally:
        db.close()

def cmd_publish(args):
    init_db()
    db: Session = SessionLocal()
    try:
        res = publish_outbox(db, batch_size=args.batch_size, topic=settings.TOPIC_ORDERS)
        print(res)
    finally:
        db.close()

def cmd_consume(args):
    res = consume_to_warehouse(topic=settings.TOPIC_ORDERS, group=args.group, max_messages=args.max_messages)
    print(res)

def cmd_query(args):
    from cdc.warehouse.duckdb_store import DuckDBWarehouse
    wh = DuckDBWarehouse(settings.DUCKDB_PATH)
    df = wh.query_orders(limit=args.limit)
    print(df.to_string(index=False))
    wh.close()

def main():
    p = argparse.ArgumentParser(prog="cdc")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("seed")
    s.add_argument("--count", type=int, default=25)
    s.set_defaults(fn=cmd_seed)

    u = sub.add_parser("update")
    u.add_argument("--order-id", required=True)
    u.add_argument("--status", choices=["CREATED","PAID","SHIPPED","DELIVERED","CANCELLED"])
    u.add_argument("--amount-kobo", type=int)
    u.add_argument("--customer-id")
    u.set_defaults(fn=cmd_update)

    pub = sub.add_parser("publish")
    pub.add_argument("--batch-size", type=int, default=200)
    pub.set_defaults(fn=cmd_publish)

    c = sub.add_parser("consume")
    c.add_argument("--group", default="cdc-warehouse-consumer")
    c.add_argument("--max-messages", type=int, default=200, help="0=run forever")
    c.set_defaults(fn=cmd_consume)

    q = sub.add_parser("query")
    q.add_argument("--limit", type=int, default=20)
    q.set_defaults(fn=cmd_query)

    args = p.parse_args()
    args.fn(args)

if __name__ == "__main__":
    main()
