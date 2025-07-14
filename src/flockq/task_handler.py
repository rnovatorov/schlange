import abc

from .task import Task


class TaskHandler(abc.ABC):

    @staticmethod
    @abc.abstractmethod
    def task_kind() -> str:
        pass

    @abc.abstractmethod
    def handle_task(self, task: Task) -> None:
        pass
