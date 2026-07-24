import dataclasses
import datetime
import uuid
from typing import List, Optional

from schlange.internal import core as internal_core

from .cleanup_policy import CleanupPolicy
from .task import Task
from .task_repository import TaskRepository
from .task_specification import TaskSpecification
from .task_state import TaskState


@dataclasses.dataclass
class TaskService:

    task_repository: TaskRepository

    def create_task(
        self,
        args: internal_core.DTO,
        kind: str,
        delay: float,
        retry_policy: internal_core.RetryPolicy,
        id: Optional[str] = None,
        schedule_id: Optional[str] = None,
    ) -> Task:
        """
        Raises:
            IOError: IO error occurred during the operation.
        """
        if id is None:
            id = str(uuid.uuid4())
        task = Task.create(
            now=self._now(),
            id=id,
            kind=kind,
            args=args,
            delay=delay,
            retry_policy=retry_policy,
            schedule_id=schedule_id,
        )
        self.task_repository.create_task(task)
        return task

    def task(self, task_id: str) -> Task:
        """
        Raises:
            IOError: IO error occurred during the operation.
            TaskNotFoundError: Task was not found.
        """
        return self.task_repository.get_task(task_id)

    def deletable_tasks(self, cleanup_policy: CleanupPolicy) -> List[Task]:
        """
        Raises:
            IOError: IO error occurred during the operation.
        """
        failed_deadline = cleanup_policy.failed_deadline(self._now())
        succeeded_deadline = cleanup_policy.succeeded_deadline(self._now())
        return self.task_repository.list_tasks(
            TaskSpecification(
                state=TaskState.FAILED,
                last_execution_ended_before=failed_deadline,
            ),
        ) + self.task_repository.list_tasks(
            TaskSpecification(
                state=TaskState.SUCCEEDED,
                last_execution_ended_before=succeeded_deadline,
            ),
        )

    def delete_task(self, task_id: str) -> None:
        """
        Raises:
            IOError: IO error occurred during the operation.
            TaskNotFoundError: Task was not found.
        """
        self.task_repository.delete_task(task_id)

    def list_tasks(self, spec: TaskSpecification) -> List[Task]:
        """
        Raises:
            IOError: IO error occurred during the operation.
        """
        return self.task_repository.list_tasks(spec)

    def begin_execution(self, task_id: str) -> Task:
        """Creates a begun execution record.

        Raises:
            IOError: IO error occurred during the operation.
            TaskNotActiveError: Task is not in active state.
            TaskNotReadyError: Task is not ready yet.
            TaskNotFoundError: Task was not found.
            TaskUpdatedConcurrentlyError: Task was updated by another transaction.
        """
        task = self.task_repository.get_task(task_id)
        task.begin_execution(now=self._now(), execution_id=str(uuid.uuid4()))
        self.task_repository.update_task(task, synchronous=False)
        return task

    def end_execution(
        self, task_id: str, execution_id: str, error: Optional[str]
    ) -> Task:
        """
        Raises:
            IOError: IO error occurred during the operation.
            TaskNotFoundError: Task was not found.
            TaskExecutionNotFoundError: Execution was not found.
            TaskUpdatedConcurrentlyError: Task was updated by another transaction.
        """
        task = self.task_repository.get_task(task_id)
        task.end_execution(execution_id=execution_id, now=self._now(), error=error)
        self.task_repository.update_task(task, synchronous=True)
        return task

    def executable_tasks(self) -> List[Task]:
        raise NotImplementedError("Executor rewrite pending")

    def execute_task(self, task_id: str) -> Task:
        raise NotImplementedError("Executor rewrite pending")

    def reactivate_task(self, task_id: str, delay: float) -> Task:
        """
        Raises:
            IOError: IO error occurred during the operation.
            TaskNotFoundError: Task was not found.
            TaskNotFailedError: Task is not in failed state.
        """
        task = self.task_repository.get_task(task_id)
        task.reactivate(now=self._now(), delay=delay)
        self.task_repository.update_task(task, synchronous=True)
        return task

    def _now(self) -> datetime.datetime:
        return datetime.datetime.now(datetime.UTC)
