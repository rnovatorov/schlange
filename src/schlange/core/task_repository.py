from typing import List, Protocol

from .task import Task
from .task_specification import TaskSpecification


class TaskRepository(Protocol):

    def create_task(self, task: Task) -> None:
        pass

    def get_task(self, task_id: str) -> Task:
        pass

    def list_tasks(self, spec: TaskSpecification) -> List[Task]:
        pass

    def delete_task(self, task_id: str) -> None:
        pass

    def update_task(self, task: Task, synchronous: bool) -> None:
        pass
