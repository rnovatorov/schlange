import dataclasses
import datetime
import traceback
import uuid
from typing import Generator, Optional

from .cleanup_policy import CleanupPolicy
from .errors import TaskLocked, TaskNotActive, TaskNotFound, TaskNotReady
from .retry_policy import RetryPolicy
from .task import Task
from .task_args import TaskArgs
from .task_executor import TaskExecutor
from .task_repository import TaskRepository
from .task_specification import TaskIsDeletable, TaskIsExecutable


@dataclasses.dataclass
class Queue:

    task_repository: TaskRepository
    retry_policy: RetryPolicy
    cleanup_policy: CleanupPolicy

    def create_task(
        self,
        args: TaskArgs,
        delay: float,
        retry_policy: Optional[RetryPolicy] = None,
    ) -> Task:
        task = Task.create(
            now=self._now(),
            id=str(uuid.uuid4()),
            args=args,
            delay=delay,
            retry_policy=(
                retry_policy if retry_policy is not None else self.retry_policy
            ),
        )
        self.task_repository.add_task(task)
        return task

    def find_deletable_tasks(self) -> Generator[Task]:
        return self.task_repository.list_tasks(
            spec=TaskIsDeletable(self._now(), self.cleanup_policy)
        )

    def delete_task(self, task_id: str) -> bool:
        try:
            self.task_repository.delete_task(task_id)
            return True
        except TaskNotFound:
            return False

    def find_executable_tasks(self) -> Generator[Task]:
        return self.task_repository.list_tasks(spec=TaskIsExecutable(self._now()))

    def execute_task(self, task_id: str, executor: TaskExecutor) -> bool:
        try:
            with self.task_repository.update_task(task_id) as task:
                try:
                    task.begin_execution(now=self._now())
                except TaskNotActive:
                    return False
                except TaskNotReady:
                    return False
                error: Optional[str] = None
                try:
                    executor(task)
                except Exception:
                    error = traceback.format_exc()
                task.end_execution(now=self._now(), error=error)
                return True
        except TaskLocked:
            return False
        except TaskNotFound:
            return False

    def _now(self) -> datetime.datetime:
        return datetime.datetime.now(datetime.UTC)
