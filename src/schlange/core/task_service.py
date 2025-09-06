import dataclasses
import datetime
import traceback
import uuid
from typing import List, Optional

from .cleanup_policy import CleanupPolicy
from .dto import DTO
from .errors import TaskHandlerNotFound
from .retry_policy import RetryPolicy
from .task import Task
from .task_handler import TaskHandler
from .task_repository import TaskRepository
from .task_specification import TaskSpecification
from .task_state import TaskState


@dataclasses.dataclass
class TaskService:

    task_repository: TaskRepository
    task_handler: Optional[TaskHandler]

    def create_task(
        self,
        args: DTO,
        delay: float,
        retry_policy: RetryPolicy,
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

    def executable_tasks(self) -> List[Task]:
        """
        Raises:
            IOError: IO error occurred during the operation.
        """
        return self.task_repository.list_tasks(
            TaskSpecification(state=TaskState.ACTIVE, ready_as_of=self._now()),
        )

    def execute_task(self, task_id: str) -> Task:
        """
        Raises:
            IOError: IO error occurred during the operation.
            TaskNotActiveError: Task is not in active state.
            TaskNotReadyError: Task is not ready yet.
            TaskNotFoundError: Task was not found.
            TaskHandlerNotFound: Task handler was not found.
            TaskUpdatedConcurrentlyError: Task was updated by another transaction.
        """
        task = self.task_repository.get_task(task_id)
        if self.task_handler is None:
            raise TaskHandlerNotFound()
        task.begin_execution(now=self._now())
        error: Optional[str] = None
        try:
            self.task_handler(task)
        except Exception:
            error = traceback.format_exc()
        task.end_execution(now=self._now(), error=error)
        self.task_repository.update_task(task, synchronous=False)
        return task

    def _now(self) -> datetime.datetime:
        return datetime.datetime.now(datetime.UTC)
