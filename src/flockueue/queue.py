import dataclasses
import datetime
import traceback
import uuid
from typing import Optional

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
        ready_at = datetime.datetime.now(datetime.UTC) + datetime.timedelta(
            seconds=delay
        )
        if retry_policy is None:
            retry_policy = self.retry_policy
        task = Task.new(id=id, args=args, ready_at=ready_at, retry_policy=retry_policy)
        self.task_repository.add_task(task)

    def execute_task(self, executor: TaskExecutor) -> bool:
        with self.task_repository.update_task(
            TaskSpecification(
                state=TaskState.ACTIVE,
                ready_as_of=datetime.datetime.now(datetime.UTC),
            )
        ) as task:
            if task is None:
                return False
            task.begin_execution(timestamp=datetime.datetime.now(datetime.UTC))
            error: Optional[str] = None
            try:
                executor(task.id, task.args)
            except Exception:
                error = traceback.format_exc()
            task.end_execution(
                timestamp=datetime.datetime.now(datetime.UTC), error=error
            )
            return True
