from pydantic import BaseModel
from typing import Optional, Literal

OpType = Literal["upsert", "delete"]

class CDCEvent(BaseModel):
    offset: int
    dataset: str
    record_id: str
    op: OpType
    payload: Optional[str]
    version: int
    event_ts: int
