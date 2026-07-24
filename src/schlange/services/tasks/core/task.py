import dataclasses
import datetime
from typing import List, Optional

from schlange.internal import core as internal_core

from .errors import (
    TaskExecutionNotEndedYetError,
    TaskExecutionNotFoundError,
    TaskNotActiveError,
    TaskNotFailedError,
    TaskNotReadyError,
)
from .task_execution import TaskExecution
from .task_state import TaskState


@dataclasses.dataclass
class Task(internal_core.Aggregate):

    created_at: datetime.datetime
    state: TaskState
    kind: str
    args: internal_core.DTO
    ready_at: datetime.datetime
    retry_policy: internal_core.RetryPolicy
    executions: List[TaskExecution]
    schedule_id: Optional[str]

    @classmethod
    def create(
        cls,
        now: datetime.datetime,
        id: str,
        kind: str,
        args: internal_core.DTO,
        delay: float,
        retry_policy: internal_core.RetryPolicy,
        schedule_id: Optional[str],
    ) -> "Task":
        return cls(
            id=id,
            version=1,
            created_at=now,
            state=TaskState.ACTIVE,
            kind=kind,
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
        """Begins a new execution record. Task must be active and ready."""
        if self.state is not TaskState.ACTIVE:
            raise TaskNotActiveError()
        if not self.ready(now):
            raise TaskNotReadyError()
        if self.last_execution is not None and not self.last_execution.ended:
            raise TaskExecutionNotEndedYetError()
        self.executions.append(
            TaskExecution.begin(seq_num=len(self.executions), timestamp=now)
        )

    def end_execution(
        self, seq_num: int, now: datetime.datetime, error: Optional[str]
    ) -> None:
        """Ends an execution by seq_num. No-op if the execution has already ended."""
        execution = self.get_execution(seq_num)
        if execution.ended:
            return  # duplicate report from redelivery — no-op
        execution.end(timestamp=now, error=error)
        if error is None:
            self.state = TaskState.SUCCEEDED
            return
        try:
            delay = self.retry_policy.delay(attempts=len(self.executions))
            self.ready_at = now + datetime.timedelta(seconds=delay)
        except internal_core.TooManyAttemptsError:
            self.state = TaskState.FAILED

    def get_execution(self, seq_num: int) -> TaskExecution:
        for execution in self.executions:
            if execution.seq_num == seq_num:
                return execution
        raise TaskExecutionNotFoundError()

    def reactivate(self, now: datetime.datetime, delay: float) -> None:
        if self.state != TaskState.FAILED:
            raise TaskNotFailedError()
        self.state = TaskState.ACTIVE
        self.ready_at = now + datetime.timedelta(seconds=delay)
        self.executions = []
