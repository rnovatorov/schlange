import contextlib
from typing import Generator, Optional, Protocol

from .task import Task
from .task_specification import TaskSpecification


class TaskRepository(Protocol):

    def add_task(self, task: Task) -> None:
        pass

    @contextlib.contextmanager
    def update_task(self, spec: TaskSpecification) -> Generator[Optional[Task]]:
        pass
