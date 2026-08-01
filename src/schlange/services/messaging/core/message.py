import dataclasses
import datetime


@dataclasses.dataclass
class Message:
    id: str
    queue: str
    payload: bytes
    visibility_timeout: float
    delivery_count: int
    created_at: datetime.datetime
    version: int
