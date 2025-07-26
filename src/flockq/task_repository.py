import contextlib
from typing import Generator, Protocol

from .task import Task
from .task_specification import TaskSpecification
from .task_state import TaskState


class TaskRepository(Protocol):

    def add_task(self, task: Task) -> None:
        pass

    def get_task(self, task_state: TaskState, task_id: str) -> Task:
        pass

    def list_tasks(
        self, task_state: TaskState, spec: TaskSpecification
    ) -> Generator[Task]:
        pass

    def delete_task(self, task_state: TaskState, task_id: str) -> None:
        pass

    @contextlib.contextmanager
    def update_task(self, task_state: TaskState, task_id: str) -> Generator[Task]:
        pass
