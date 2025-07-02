import contextlib
from typing import Generator, Protocol

from .task import Task
from .task_specification import TaskSpecification


class TaskRepository(Protocol):

    def add_task(self, task: Task) -> None:
        pass

    def list_tasks(self, spec: TaskSpecification) -> Generator[Task]:
        pass

    def delete_task(self, task_id: str) -> None:
        pass

    @contextlib.contextmanager
    def update_task(self, task_id: str) -> Generator[Task]:
        pass
