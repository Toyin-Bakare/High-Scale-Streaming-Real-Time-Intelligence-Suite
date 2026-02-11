from __future__ import annotations
from typing import Dict, Optional, Any
from pydantic import BaseModel, Field
from datetime import datetime

class Event(BaseModel):
    event_id: str
    event_type: str
    metric: str
    value: float = 1.0
    user_id: Optional[str] = None
    customer_id: Optional[str] = None
    endpoint: Optional[str] = None
    status: Optional[str] = None
    ts: datetime
    extra: Dict[str, Any] = Field(default_factory=dict)

class Rollup(BaseModel):
    metric: str
    window_start: datetime
    window_end: datetime
    dims: Dict[str, str] = Field(default_factory=dict)
    count: int
    sum: float
    uniques: int
