from __future__ import annotations
from typing import Dict, Any, List
from datetime import datetime
from sqlalchemy import create_engine, Column, String, DateTime, Integer, Float, JSON, text
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from aggregator.windowing import dims_hash
import json

class Base(DeclarativeBase):
    pass

class RollupRow(Base):
    __tablename__ = "rollups"
    window_start = Column(DateTime(timezone=True), primary_key=True)
    window_end = Column(DateTime(timezone=True), primary_key=True)
    metric = Column(String(80), primary_key=True)
    dims_hash = Column(String(32), primary_key=True)
    dims = Column(JSON, nullable=False)
    count = Column(Integer, nullable=False)
    sum = Column(Float, nullable=False)
    uniques = Column(Integer, nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False)

class PostgresSink:
    def __init__(self, url: str):
        self.engine = create_engine(url, pool_pre_ping=True)
        Base.metadata.create_all(bind=self.engine)

    def upsert_rollups(self, rollups: List[Dict[str, Any]]) -> None:
        if not rollups:
            return
        with self.engine.begin() as conn:
            for r in rollups:
                dh = dims_hash(r["dims"])
                conn.execute(text("""
                INSERT INTO rollups(window_start, window_end, metric, dims_hash, dims, count, sum, uniques, updated_at)
                VALUES (:ws, :we, :metric, :dh, :dims::jsonb, :count, :sum, :uniques, :updated_at)
                ON CONFLICT (window_start, window_end, metric, dims_hash)
                DO UPDATE SET
                  dims = EXCLUDED.dims,
                  count = EXCLUDED.count,
                  sum = EXCLUDED.sum,
                  uniques = EXCLUDED.uniques,
                  updated_at = EXCLUDED.updated_at;
                """), {
                    "ws": r["window_start"],
                    "we": r["window_end"],
                    "metric": r["metric"],
                    "dh": dh,
                    "dims": json.dumps(r["dims"], sort_keys=True),
                    "count": int(r["count"]),
                    "sum": float(r["sum"]),
                    "uniques": int(r["uniques"]),
                    "updated_at": r["updated_at"],
                })

    def query_history(self, metric: str, from_ts: datetime, to_ts: datetime, limit: int = 500):
        sql = text("""
        SELECT window_start, window_end, metric, dims, count, sum, uniques
        FROM rollups
        WHERE metric = :metric
          AND window_start >= :from_ts
          AND window_start < :to_ts
        ORDER BY window_start DESC
        LIMIT :limit;
        """)
        with self.engine.begin() as conn:
            rows = conn.execute(sql, {"metric": metric, "from_ts": from_ts, "to_ts": to_ts, "limit": limit}).mappings().all()
            return [dict(r) for r in rows]
