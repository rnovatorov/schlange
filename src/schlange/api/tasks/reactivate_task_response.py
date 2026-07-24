import dataclasses

from .task import Task


@dataclasses.dataclass
class ReactivateTaskResponse:
    task: Task
