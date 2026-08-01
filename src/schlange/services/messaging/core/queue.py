import dataclasses
import datetime
from typing import Optional


@dataclasses.dataclass
class Queue:
    name: str
    dead_letter_queue: Optional[str]
    visibility_timeout: float
    created_at: datetime.datetime
