import dataclasses
import datetime
import traceback
import uuid
from typing import Generator, Optional

from .errors import TaskLocked, TaskNotFound
from .retry_policy import DEFAULT_RETRY_POLICY, RetryPolicy
from .task import Task
from .task_args import TaskArgs
from .task_executor import TaskExecutor
from .task_repository import TaskRepository
from .task_specification import TaskSpecification
from .task_state import TaskState


@dataclasses.dataclass
class Queue:

    task_repository: TaskRepository
    retry_policy: RetryPolicy

    @classmethod
    def new(
        cls,
        task_repository: TaskRepository,
        retry_policy: RetryPolicy = DEFAULT_RETRY_POLICY,
    ) -> "Queue":
        return cls(task_repository=task_repository, retry_policy=retry_policy)

    def create_task(
        self,
        args: TaskArgs,
        delay: float = 0.0,
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

    def find_executable_tasks(self) -> Generator[Task]:
        spec = TaskSpecification(state=TaskState.ACTIVE, ready_as_of=self._now())
        return self.task_repository.list_tasks(spec=spec)

    def execute_task(self, task_id: str, executor: TaskExecutor) -> bool:
        spec = TaskSpecification(state=TaskState.ACTIVE, ready_as_of=self._now())
        try:
            with self.task_repository.update_task(task_id) as task:
                if not spec.is_satisfied_by(task):
                    return False
                task.begin_execution(now=self._now())
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
