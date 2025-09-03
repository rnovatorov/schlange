import dataclasses
import datetime
from typing import Optional


@dataclasses.dataclass
class TaskExecution:

    begun_at: datetime.datetime
    ended_at: Optional[datetime.datetime]
    error: Optional[str]

    @classmethod
    def begin(cls, timestamp: datetime.datetime) -> "TaskExecution":
        return cls(begun_at=timestamp, ended_at=None, error=None)

    def end(self, timestamp: datetime.datetime, error: Optional[str]) -> None:
        self.ended_at = timestamp
        self.error = error

    @property
    def ended(self) -> bool:
        return self.ended_at is not None

    @property
    def duration(self) -> Optional[datetime.timedelta]:
        if self.ended_at is None:
            return None
        return self.ended_at - self.begun_at
