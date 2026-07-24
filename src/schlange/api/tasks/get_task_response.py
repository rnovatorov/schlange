import dataclasses

from .task import Task


@dataclasses.dataclass
class GetTaskResponse:
    task: Task
