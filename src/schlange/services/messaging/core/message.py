import dataclasses
import datetime


@dataclasses.dataclass
class Message:
    id: str
    queue: str
    payload: bytes
    created_at: datetime.datetime
    version: int
