from app.db import init_db, connect
from app.cdc import upsert_source_and_emit
from app.indexer import run_once

def test_idempotent_indexing_ignores_duplicates(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "test.sqlite3"))
    init_db()

    upsert_source_and_emit("cases", "CASE-9", "v1")
    upsert_source_and_emit("cases", "CASE-9", "v2")

    run_once(["cases"])
    run_once(["cases"])

    with connect() as conn:
        row = conn.execute(
            "SELECT payload, version, tombstone FROM index_records WHERE dataset='cases' AND record_id='CASE-9'"
        ).fetchone()
        assert row is not None
        assert row[0] == "v2"
        assert int(row[1]) == 2
        assert int(row[2]) == 0
