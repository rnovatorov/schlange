import contextlib
from typing import Generator, Optional, Protocol

from .task import Task
from .task_specification import TaskSpecification


class TaskRepository(Protocol):

    def add_task(self, task: Task) -> None:
        pass

    def list_tasks(self, spec: Optional[TaskSpecification] = None) -> Generator[Task]:
        pass

    @contextlib.contextmanager
    def update_task(self, task_id: str) -> Generator[Task]:
        pass
