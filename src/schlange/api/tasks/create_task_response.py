import dataclasses

from .task import Task


@dataclasses.dataclass
class CreateTaskResponse:
    task: Task
