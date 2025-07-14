import dataclasses
import datetime
from typing import List

from .retry_policy import RetryPolicy
from .task_args import TaskArgs
from .task_execution import TaskExecution
from .task_state import TaskState


@dataclasses.dataclass
class TaskProjection:

    created_at: datetime.datetime
    kind: str
    args: TaskArgs
    state: TaskState
    ready_at: datetime.datetime
    retry_policy: RetryPolicy
    executions: List[TaskExecution]
