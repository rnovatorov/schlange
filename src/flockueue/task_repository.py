import contextlib
from typing import Generator, Protocol

from .task import Task


class TaskRepository(Protocol):

    def add_task(self, task: Task) -> None:
        pass

    def list_tasks(self) -> Generator[str]:
        pass

    @contextlib.contextmanager
    def update_task(self, task_id: str) -> Generator[Task]:
        pass
