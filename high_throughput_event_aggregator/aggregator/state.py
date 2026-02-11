from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Optional, Tuple, List
from datetime import datetime, timezone, timedelta
from aggregator.windowing import dims_hash

@dataclass
class AggCell:
    count: int = 0
    sum: float = 0.0
    unique_users: set = None

    def __post_init__(self):
        if self.unique_users is None:
            self.unique_users = set()

class WindowState:
    def __init__(self, retention_windows: int = 20):
        self.cells: Dict[Tuple[str, str, str], AggCell] = {}
        self.dims_by_key: Dict[Tuple[str, str, str], Dict[str, str]] = {}
        self.window_end_by_ws: Dict[str, datetime] = {}
        self.retention_windows = retention_windows

    def update(self, metric: str, window_start: datetime, window_end: datetime, dims: Dict[str, str],
               value: float, user_id: Optional[str]) -> None:
        ws = window_start.astimezone(timezone.utc).isoformat()
        dh = dims_hash(dims)
        key = (metric, ws, dh)
        cell = self.cells.get(key)
        if cell is None:
            cell = AggCell()
            self.cells[key] = cell
            self.dims_by_key[key] = dims
        cell.count += 1
        cell.sum += float(value)
        if user_id:
            cell.unique_users.add(str(user_id))
        self.window_end_by_ws[ws] = window_end

    def snapshot_ready(self, now: datetime, flush_lag_seconds: int = 0) -> List[Tuple[str, datetime, datetime, Dict[str,str], AggCell]]:
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        cutoff = now - timedelta(seconds=flush_lag_seconds)
        ready = []
        for (metric, ws, dh), cell in list(self.cells.items()):
            wend = self.window_end_by_ws.get(ws)
            if wend and wend <= cutoff:
                dims = self.dims_by_key.get((metric, ws, dh), {})
                wstart = datetime.fromisoformat(ws)
                ready.append((metric, wstart, wend, dims, cell))
        return ready

    def evict_older_than(self, keep_after: datetime) -> int:
        removed = 0
        for key in list(self.cells.keys()):
            ws = key[1]
            wend = self.window_end_by_ws.get(ws)
            if wend and wend <= keep_after:
                self.cells.pop(key, None)
                self.dims_by_key.pop(key, None)
                removed += 1
        for ws in list(self.window_end_by_ws.keys()):
            if self.window_end_by_ws[ws] <= keep_after:
                self.window_end_by_ws.pop(ws, None)
        return removed
