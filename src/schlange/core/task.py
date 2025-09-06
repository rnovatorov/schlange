import dataclasses
import datetime
from typing import List, Optional

from .aggregate import Aggregate
from .dto import DTO
from .errors import TaskNotActiveError, TaskNotReadyError, TooManyAttemptsError
from .retry_policy import RetryPolicy
from .task_execution import TaskExecution
from .task_state import TaskState


@dataclasses.dataclass
class Task(Aggregate):

    created_at: datetime.datetime
    state: TaskState
    args: DTO
    ready_at: datetime.datetime
    retry_policy: RetryPolicy
    executions: List[TaskExecution]
    schedule_id: Optional[str]

    @classmethod
    def create(
        cls,
        now: datetime.datetime,
        id: str,
        args: DTO,
        delay: float,
        retry_policy: RetryPolicy,
        schedule_id: Optional[str],
    ) -> "Task":
        return cls(
            id=id,
            version=1,
            created_at=now,
            state=TaskState.ACTIVE,
            args=args,
            ready_at=now + datetime.timedelta(seconds=delay),
            retry_policy=retry_policy,
            executions=[],
            schedule_id=schedule_id,
        )

    def ready(self, now: datetime.datetime) -> bool:
        return self.ready_at <= now

    @property
    def last_execution(self) -> Optional[TaskExecution]:
        return self.executions[-1] if self.executions else None

    def begin_execution(self, now: datetime.datetime) -> None:
        if self.state is not TaskState.ACTIVE:
            raise TaskNotActiveError()
        if not self.ready(now):
            raise TaskNotReadyError()
        assert self.last_execution is None or self.last_execution.ended
        self.executions.append(TaskExecution.begin(timestamp=now))

    def end_execution(self, now: datetime.datetime, error: Optional[str]) -> None:
        assert self.last_execution is not None and not self.last_execution.ended
        self.last_execution.end(timestamp=now, error=error)
        if error is None:
            self.state = TaskState.SUCCEEDED
            return
        try:
            delay = self.retry_policy.delay(attempts=len(self.executions))
            self.ready_at = now + datetime.timedelta(seconds=delay)
        except TooManyAttemptsError:
            self.state = TaskState.FAILED
