import dataclasses
from typing import Protocol

from schlange.internal import core as internal_core


@dataclasses.dataclass
class TaskExecutionRequest:
    """Request to execute a task, handed to the message queue port."""

    task_id: str
    seq_num: int
    kind: str
    args: internal_core.DTO


class MessageQueue(Protocol):
    """Driven port for publishing task execution requests."""

    def publish(self, request: TaskExecutionRequest) -> None: ...
