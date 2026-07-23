import dataclasses
import datetime
from typing import Optional


@dataclasses.dataclass
class Message:
    id: str
    routing_key: str
    payload: bytes
    created_at: datetime.datetime
    is_dead_letter: bool
    claimed_by: Optional[str]
    claimed_at: Optional[datetime.datetime]
