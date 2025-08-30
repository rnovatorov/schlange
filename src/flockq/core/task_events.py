import dataclasses
from typing import Optional

from .event import Event
from .retry_policy import RetryPolicy
from .task_args import TaskArgs


@dataclasses.dataclass
class TaskCreated(Event):

    kind: str
    args: TaskArgs
    delay: float
    retry_policy: RetryPolicy


@dataclasses.dataclass
class TaskExecutionBegun(Event):
    pass


@dataclasses.dataclass
class TaskExecutionEnded(Event):

    error: Optional[str]


@dataclasses.dataclass
class TaskSucceeded(Event):
    pass


@dataclasses.dataclass
class TaskDelayed(Event):

    delay: float


@dataclasses.dataclass
class TaskFailed(Event):
    pass
