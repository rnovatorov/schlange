import dataclasses
from typing import Any, Dict

from .task_state import TaskState


@dataclasses.dataclass
class Task:
    id: str
    kind: str
    args: Dict[str, Any]
    state: TaskState
