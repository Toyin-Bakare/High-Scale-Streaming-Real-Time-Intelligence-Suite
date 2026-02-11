from app.db import init_db
from app.cdc import upsert_source_and_emit
from app.indexer import run_once
from app.monitor import get_dataset_metrics

def test_freshness_lag_decreases_after_indexing(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "test.sqlite3"))
    init_db()

    upsert_source_and_emit("cases", "CASE-10", "hello")

    m1 = get_dataset_metrics("cases")
    assert m1["max_offset"] >= 1
    assert m1["backlog_offsets"] >= 1

    run_once(["cases"])
    m2 = get_dataset_metrics("cases")
    assert m2["backlog_offsets"] == 0
    assert m2["checkpoint_last_event_ts"] == m2["max_event_ts"]
