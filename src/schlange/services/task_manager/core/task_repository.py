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
        """
        If synchronous is True, the write is durable before this
        method returns. If False, the write may be lost in a crash;
        crash recovery is expected to re-execute any lost work.
        In both cases, the write is visible to other connections
        once the method returns.
        """
        pass
