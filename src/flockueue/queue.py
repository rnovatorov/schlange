import dataclasses
import datetime
import traceback
import uuid
from typing import Optional

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

    def add_task(
        self,
        args: TaskArgs,
        id: Optional[str] = None,
        delay: float = 0.0,
        retry_policy: Optional[RetryPolicy] = None,
    ) -> None:
        if id is None:
            id = str(uuid.uuid4())
        if retry_policy is None:
            retry_policy = self.retry_policy
        task = Task.create(
            now=datetime.datetime.now(datetime.UTC),
            id=id,
            args=args,
            delay=delay,
            retry_policy=retry_policy,
        )
        self.task_repository.add_task(task)

    def execute_tasks(self, executor: TaskExecutor) -> bool:
        spec = TaskSpecification(state=TaskState.ACTIVE, ready_as_of=self._now())
        queue = self.task_repository.list_tasks()
        progress_made = False
        for task_id in queue:
            try:
                with self.task_repository.update_task(task_id) as task:
                    if not spec.is_satisfied_by(task):
                        continue
                    task.begin_execution(now=self._now())
                    error: Optional[str] = None
                    try:
                        executor(task.id, task.args)
                    except Exception:
                        error = traceback.format_exc()
                    task.end_execution(now=self._now(), error=error)
                    progress_made = True
            except TaskLocked:
                continue
            except TaskNotFound:
                continue
        return progress_made

    def _now(self) -> datetime.datetime:
        return datetime.datetime.now(datetime.UTC)
