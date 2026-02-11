import sqlite3
from contextlib import contextmanager
from app.config import settings

SCHEMA = """
PRAGMA journal_mode=WAL;

-- Source-of-truth table
CREATE TABLE IF NOT EXISTS source_records (
  dataset TEXT NOT NULL,
  record_id TEXT NOT NULL,
  payload TEXT NOT NULL,
  version INTEGER NOT NULL,
  updated_at INTEGER NOT NULL, -- unix epoch seconds
  PRIMARY KEY (dataset, record_id)
);

-- CDC event log (append-only)
CREATE TABLE IF NOT EXISTS cdc_events (
  offset INTEGER PRIMARY KEY AUTOINCREMENT,
  dataset TEXT NOT NULL,
  record_id TEXT NOT NULL,
  op TEXT NOT NULL,               -- upsert/delete
  payload TEXT,                   -- null if delete
  version INTEGER NOT NULL,
  event_ts INTEGER NOT NULL       -- unix epoch seconds
);

-- "Search index" table (the derived/served layer)
CREATE TABLE IF NOT EXISTS index_records (
  dataset TEXT NOT NULL,
  record_id TEXT NOT NULL,
  payload TEXT,
  version INTEGER NOT NULL,
  indexed_at INTEGER NOT NULL,    -- unix epoch seconds
  tombstone INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (dataset, record_id)
);

-- Consumer checkpoint (per dataset)
CREATE TABLE IF NOT EXISTS consumer_checkpoint (
  dataset TEXT PRIMARY KEY,
  last_offset INTEGER NOT NULL,
  last_event_ts INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
"""

@contextmanager
def connect():
    conn = sqlite3.connect(settings.DB_PATH)
    try:
        yield conn
    finally:
        conn.close()

def init_db():
    with connect() as conn:
        conn.executescript(SCHEMA)
        conn.commit()
