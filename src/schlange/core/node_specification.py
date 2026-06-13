import dataclasses
import datetime
from typing import Optional

from .node import Node


@dataclasses.dataclass
class NodeSpecification:

    last_heartbeat_before: Optional[datetime.datetime] = None

    def is_satisfied_by(self, node: Node) -> bool:
        preconditions = [
            (
                self.last_heartbeat_before is None
                or node.last_heartbeat_at < self.last_heartbeat_before
            ),
        ]
        return all(preconditions)
