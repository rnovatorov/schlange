import dataclasses
import datetime


@dataclasses.dataclass
class Lease:
    key: str
    holder: str
    expires_at: datetime.datetime
