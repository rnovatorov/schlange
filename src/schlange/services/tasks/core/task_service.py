import dataclasses
import datetime
import uuid
from typing import List, Optional

from schlange.internal import core as internal_core

from .cleanup_policy import CleanupPolicy
from .lease_service import LeaseService
from .message_queue import MessageQueue, TaskExecutionRequest
from .task import Task
from .task_repository import TaskRepository
from .task_specification import TaskSpecification
from .task_state import TaskState


@dataclasses.dataclass
class TaskService:

    task_repository: TaskRepository
    message_queue: MessageQueue
    lease_service: LeaseService

    def create_task(
        self,
        args: internal_core.DTO,
        kind: str,
        delay: float,
        visibility_timeout: float,
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
            visibility_timeout=visibility_timeout,
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

    def list_tasks(self, spec: TaskSpecification) -> List[Task]:
        """
        Raises:
            IOError: IO error occurred during the operation.
        """
        return self.task_repository.list_tasks(spec)

    def delete_task(self, task_id: str) -> None:
        """
        Raises:
            IOError: IO error occurred during the operation.
            TaskNotFoundError: Task was not found.
        """
        self.task_repository.delete_task(task_id)

    def begin_execution(self, task_id: str) -> None:
        """Begins an execution for a task and publishes the execution request.

        Raises:
            IOError: IO error occurred during the operation.
            TaskNotFoundError: Task was not found.
            TaskNotActiveError: Task is not in active state.
            TaskNotReadyError: Task is not ready yet.
            TaskExecutionNotEndedYetError: Task has an outstanding execution.
            TaskUpdatedConcurrentlyError: Task was updated by another transaction.
        """
        task = self.task_repository.get_task(task_id)
        task.begin_execution(now=self._now())
        execution = task.last_execution
        assert execution is not None
        self.message_queue.publish(
            TaskExecutionRequest(
                task_id=task.id,
                seq_num=execution.seq_num,
                kind=task.kind,
                args=task.args,
                visibility_timeout=task.visibility_timeout,
            )
        )
        self.task_repository.update_task(task, synchronous=False)

    def end_execution(self, task_id: str, seq_num: int, error: Optional[str]) -> Task:
        """
        Raises:
            IOError: IO error occurred during the operation.
            TaskNotFoundError: Task was not found.
            TaskExecutionNotFoundError: Execution was not found.
            TaskUpdatedConcurrentlyError: Task was updated by another transaction.
        """
        task = self.task_repository.get_task(task_id)
        task.end_execution(seq_num=seq_num, now=self._now(), error=error)
        self.task_repository.update_task(task, synchronous=True)
        return task

    def executable_tasks(self) -> List[Task]:
        return self.task_repository.list_tasks(
            TaskSpecification(
                state=TaskState.ACTIVE,
                ready_as_of=self._now(),
                execution_in_progress=False,
            )
        )

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

    def acquire_lease(self, key: str, holder: str, ttl: float) -> bool:
        """Acquire or renew a lease via the lease service port."""
        return self.lease_service.acquire_lease(key=key, holder=holder, ttl=ttl)

    def _now(self) -> datetime.datetime:
        return datetime.datetime.now(datetime.UTC)
