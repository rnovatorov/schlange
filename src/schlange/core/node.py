import dataclasses
import datetime

from .aggregate import Aggregate


@dataclasses.dataclass
class Node(Aggregate):

    last_heartbeat_at: datetime.datetime

    @classmethod
    def create(cls, now: datetime.datetime, id: str) -> "Node":
        return cls(id=id, version=1, last_heartbeat_at=now)

    def heartbeat(self, now: datetime.datetime) -> None:
        self.last_heartbeat_at = now
