import abc

from .task import Task


class TaskHandler(abc.ABC):

    @abc.abstractmethod
    def handle_task(self, task: Task) -> None:
        pass
