import dataclasses
import datetime
import traceback
import uuid
from typing import Generator, Optional

from .cleanup_policy import CleanupPolicy
from .errors import TaskHandlerNotFound, TaskNotFoundError
from .retry_policy import RetryPolicy
from .task import Task
from .task_args import TaskArgs
from .task_handler import TaskHandler
from .task_handler_registry import TaskHandlerRegistry
from .task_repository import TaskRepository
from .task_specification import TaskSpecification
from .task_state import TaskState


@dataclasses.dataclass
class TaskService:

    task_repository: TaskRepository
    task_handler_registry: TaskHandlerRegistry

    def register_task_handler(self, task_kind: str, task_handler: TaskHandler) -> None:
        self.task_handler_registry.register_task_handler(task_kind, task_handler)

    def create_task(
        self, kind: str, args: TaskArgs, delay: float, retry_policy: RetryPolicy
    ) -> Task:
        """
        Raises:
            IOError: IO error occurred during the operation.
        """
        task = Task.create(
            now=self._now(),
            id=str(uuid.uuid4()),
            kind=kind,
            args=args,
            delay=delay,
            retry_policy=retry_policy,
        )
        self.task_repository.add_task(task)
        return task

    def task(self, task_id: str) -> Task:
        """
        Raises:
            IOError: IO error occurred during the operation.
            TaskNotFoundError: Task was not found.
        """
        for task_state in TaskState:
            try:
                return self.task_repository.get_task(task_state, task_id)
            except TaskNotFoundError:
                continue
        raise TaskNotFoundError()

    def deletable_failed_tasks(self, cleanup_policy: CleanupPolicy) -> Generator[Task]:
        """
        Raises:
            IOError: IO error occurred during the operation.
        """
        deadline = cleanup_policy.failed_deadline(self._now())
        return self.task_repository.list_tasks(
            TaskState.FAILED, TaskSpecification(last_execution_ended_before=deadline)
        )

    def deletable_succeeded_tasks(
        self, cleanup_policy: CleanupPolicy
    ) -> Generator[Task]:
        """
        Raises:
            IOError: IO error occurred during the operation.
        """
        deadline = cleanup_policy.succeeded_deadline(self._now())
        return self.task_repository.list_tasks(
            TaskState.SUCCEEDED, TaskSpecification(last_execution_ended_before=deadline)
        )

    def delete_succeeded_task(self, task_id: str) -> None:
        """
        Raises:
            IOError: IO error occurred during the operation.
            TaskNotFoundError: Task was not found.
        """
        self.task_repository.delete_task(TaskState.SUCCEEDED, task_id)

    def delete_failed_task(self, task_id: str) -> None:
        """
        Raises:
            IOError: IO error occurred during the operation.
            TaskNotFoundError: Task was not found.
        """
        self.task_repository.delete_task(TaskState.FAILED, task_id)

    def executable_tasks(self) -> Generator[Task]:
        """
        Raises:
            IOError: IO error occurred during the operation.
        """
        return self.task_repository.list_tasks(
            TaskState.ACTIVE,
            TaskSpecification(
                ready_as_of=self._now(), kind_in=self.task_handler_registry.task_kinds()
            ),
        )

    def execute_task(self, task_id: str) -> Task:
        """
        Raises:
            IOError: IO error occurred during the operation.
            TaskNotActiveError: Task is not in active state.
            TaskNotReadyError: Task is not ready yet.
            TaskLockedError: Task is currently locked by another thread.
            TaskNotFoundError: Task was not found.
            TaskHandlerNotFound: Task handler was not found.
        """
        with self.task_repository.update_task(TaskState.ACTIVE, task_id) as task:
            try:
                task_handler = self.task_handler_registry.task_handler(task.kind)
            except KeyError:
                raise TaskHandlerNotFound(task.kind)
            task.begin_execution(now=self._now())
            error: Optional[str] = None
            try:
                task_handler(task)
            except Exception:
                error = traceback.format_exc()
            task.end_execution(now=self._now(), error=error)
            return task

    def _now(self) -> datetime.datetime:
        return datetime.datetime.now(datetime.UTC)
