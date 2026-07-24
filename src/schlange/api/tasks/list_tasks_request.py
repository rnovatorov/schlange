import dataclasses
import datetime
from typing import Optional

from .task_state import TaskState


@dataclasses.dataclass
class ListTasksRequest:
    state: Optional[TaskState] = None
    ready_as_of: Optional[datetime.datetime] = None
    last_execution_ended_before: Optional[datetime.datetime] = None
