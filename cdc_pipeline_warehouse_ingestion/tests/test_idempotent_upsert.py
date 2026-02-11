from cdc.warehouse.duckdb_store import DuckDBWarehouse
import tempfile, os

def test_idempotent_apply_same_event_twice():
    with tempfile.TemporaryDirectory() as td:
        dbp = os.path.join(td, "wh.duckdb")
        wh = DuckDBWarehouse(dbp)

        env = {
            "event_id": "e1",
            "event_type": "order.created",
            "aggregate_id": "o-1",
            "idempotency_key": "idem-1",
            "schema_version": 1,
            "payload": {"order_id":"o-1","customer_id":"c-1","amount_kobo":100,"currency":"NGN","status":"CREATED"},
        }

        wh.apply_order_event(env, kafka_meta={"topic":"t","partition":0,"offset":1})
        wh.apply_order_event(env, kafka_meta={"topic":"t","partition":0,"offset":2})  # replay

        df = wh.query_orders(limit=10)
        assert len(df) == 1
        assert df.iloc[0]["order_id"] == "o-1"
        wh.close()
