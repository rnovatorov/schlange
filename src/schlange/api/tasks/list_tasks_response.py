import dataclasses
from typing import List

from .task import Task


@dataclasses.dataclass
class ListTasksResponse:
    tasks: List[Task]
