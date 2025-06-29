import dataclasses
import datetime
from typing import List, Optional

from .errors import TooManyAttempts
from .retry_policy import RetryPolicy
from .task_args import TaskArgs
from .task_execution import TaskExecution
from .task_state import TaskState


@dataclasses.dataclass
class Task:

    id: str
    args: TaskArgs
    state: TaskState
    ready_at: datetime.datetime
    retry_policy: RetryPolicy
    executions: List[TaskExecution]

    @classmethod
    def new(
        cls,
        id: str,
        args: TaskArgs,
        ready_at: datetime.datetime,
        retry_policy: RetryPolicy,
    ) -> "Task":
        return Task(
            id=id,
            args=args,
            state=TaskState.ACTIVE,
            ready_at=ready_at,
            retry_policy=retry_policy,
            executions=[],
        )

    @property
    def last_execution(self) -> Optional[TaskExecution]:
        return self.executions[-1] if self.executions else None

    def begin_execution(self, timestamp: datetime.datetime) -> None:
        assert self.state is TaskState.ACTIVE
        assert self.last_execution is None or self.last_execution.ended
        self.executions.append(TaskExecution.begin(timestamp=timestamp))

    def end_execution(self, timestamp: datetime.datetime, error: Optional[str]) -> None:
        assert self.last_execution is not None
        self.last_execution.end(timestamp=timestamp, error=error)
        if self.last_execution.ok:
            self.state = TaskState.SUCCEEDED
            return
        try:
            delay = self.retry_policy.delay(attempts=len(self.executions))
        except TooManyAttempts:
            self.state = TaskState.FAILED
            return
        self.ready_at = timestamp + datetime.timedelta(seconds=delay)
