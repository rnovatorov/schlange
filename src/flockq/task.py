import datetime
from typing import List, Optional

from .aggregate import Aggregate
from .errors import TaskNotActiveError, TaskNotReadyError, TooManyAttemptsError
from .retry_policy import RetryPolicy
from .task_args import TaskArgs
from .task_events import (
    TaskCreated,
    TaskDelayed,
    TaskEvent,
    TaskExecutionBegun,
    TaskExecutionEnded,
    TaskFailed,
    TaskSucceeded,
)
from .task_execution import TaskExecution
from .task_projection import TaskProjection
from .task_state import TaskState


class Task(Aggregate[TaskEvent, TaskProjection]):

    @classmethod
    def create(
        cls,
        now: datetime.datetime,
        id: str,
        kind: str,
        args: TaskArgs,
        delay: float,
        retry_policy: RetryPolicy,
    ) -> "Task":
        task = cls(id=id)
        task._emit(
            TaskCreated(
                timestamp=now,
                kind=kind,
                args=args,
                delay=delay,
                retry_policy=retry_policy,
            )
        )
        return task

    @property
    def kind(self) -> str:
        assert self._projection is not None
        return self._projection.kind

    @property
    def args(self) -> TaskArgs:
        assert self._projection is not None
        return self._projection.args

    @property
    def ready_at(self) -> datetime.datetime:
        assert self._projection is not None
        return self._projection.ready_at

    @property
    def state(self) -> TaskState:
        assert self._projection is not None
        return self._projection.state

    @property
    def executions(self) -> List[TaskExecution]:
        assert self._projection is not None
        return self._projection.executions

    @property
    def last_execution(self) -> Optional[TaskExecution]:
        return self.executions[-1] if self.executions else None

    @property
    def retry_policy(self) -> RetryPolicy:
        assert self._projection is not None
        return self._projection.retry_policy

    def begin_execution(self, now: datetime.datetime) -> None:
        if self.state is not TaskState.ACTIVE:
            raise TaskNotActiveError()
        if not self.ready_at <= now:
            raise TaskNotReadyError()
        assert self.last_execution is None or self.last_execution.ended
        self._emit(TaskExecutionBegun(timestamp=now))

    def end_execution(self, now: datetime.datetime, error: Optional[str]) -> None:
        assert self.last_execution is not None
        assert self.last_execution.begun
        assert not self.last_execution.ended
        self._emit(TaskExecutionEnded(timestamp=now, error=error))
        if error is None:
            self._emit(TaskSucceeded(timestamp=now))
            return
        try:
            delay = self.retry_policy.delay(attempts=len(self.executions))
        except TooManyAttemptsError:
            self._emit(TaskFailed(timestamp=now))
            return
        self._emit(TaskDelayed(timestamp=now, delay=delay))

    def _apply(self, event: TaskEvent) -> None:
        if isinstance(event, TaskCreated):
            self._apply_created(event)
        elif isinstance(event, TaskExecutionBegun):
            self._apply_execution_begun(event)
        elif isinstance(event, TaskExecutionEnded):
            self._apply_execution_ended(event)
        elif isinstance(event, TaskSucceeded):
            self._apply_succeeded(event)
        elif isinstance(event, TaskFailed):
            self._apply_failed(event)
        elif isinstance(event, TaskDelayed):
            self._apply_delayed(event)
        else:
            raise TypeError(event)

    def _apply_created(self, event: TaskCreated) -> None:
        assert self._projection is None
        self._projection = TaskProjection(
            created_at=event.timestamp,
            kind=event.kind,
            args=event.args,
            state=TaskState.ACTIVE,
            ready_at=event.timestamp + datetime.timedelta(seconds=event.delay),
            retry_policy=event.retry_policy,
            executions=[],
        )

    def _apply_execution_begun(self, event: TaskExecutionBegun) -> None:
        assert self._projection is not None
        self._projection.executions.append(
            TaskExecution.begin(timestamp=event.timestamp)
        )

    def _apply_execution_ended(self, event: TaskExecutionEnded) -> None:
        assert self._projection is not None
        assert self._projection.executions
        self._projection.executions[-1].end(
            timestamp=event.timestamp, error=event.error
        )

    def _apply_succeeded(self, event: TaskSucceeded) -> None:
        assert self._projection is not None
        self._projection.state = TaskState.SUCCEEDED

    def _apply_failed(self, event: TaskFailed) -> None:
        assert self._projection is not None
        self._projection.state = TaskState.FAILED

    def _apply_delayed(self, event: TaskDelayed) -> None:
        assert self._projection is not None
        self._projection.ready_at = event.timestamp + datetime.timedelta(
            seconds=event.delay
        )
