from __future__ import annotations
from typing import Dict, Tuple
from datetime import datetime, timezone, timedelta
import hashlib, json

def window_bounds(ts: datetime, window_seconds: int) -> Tuple[datetime, datetime]:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    epoch = int(ts.timestamp())
    start_epoch = (epoch // window_seconds) * window_seconds
    start = datetime.fromtimestamp(start_epoch, tz=timezone.utc)
    end = start + timedelta(seconds=window_seconds)
    return start, end

def normalize_dims(dims: Dict[str, str]) -> Dict[str, str]:
    out = {}
    for k, v in dims.items():
        if v is None:
            continue
        s = str(v).strip()
        if s:
            out[str(k)] = s
    return out

def dims_hash(dims: Dict[str, str]) -> str:
    payload = json.dumps(dims, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:32]
