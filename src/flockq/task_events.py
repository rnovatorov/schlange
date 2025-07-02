import dataclasses
from typing import Optional

from .event import Event
from .retry_policy import RetryPolicy
from .task_args import TaskArgs


class TaskEvent(Event):
    pass


@dataclasses.dataclass
class TaskCreated(TaskEvent):

    args: TaskArgs
    delay: float
    retry_policy: RetryPolicy


@dataclasses.dataclass
class TaskExecutionBegun(TaskEvent):
    pass


@dataclasses.dataclass
class TaskExecutionEnded(TaskEvent):

    error: Optional[str]


@dataclasses.dataclass
class TaskSucceeded(TaskEvent):
    pass


@dataclasses.dataclass
class TaskDelayed(TaskEvent):

    delay: float


@dataclasses.dataclass
class TaskFailed(TaskEvent):
    pass
