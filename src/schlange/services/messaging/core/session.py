import dataclasses
import datetime


@dataclasses.dataclass
class Session:
    id: str
    queue: str
    dead_letter: bool
    last_heartbeat_at: datetime.datetime
    created_at: datetime.datetime
