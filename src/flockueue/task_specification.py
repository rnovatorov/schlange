import dataclasses
import datetime
from typing import Optional

from .task import Task
from .task_state import TaskState


@dataclasses.dataclass
class TaskSpecification:

    id: Optional[str] = None
    state: Optional[TaskState] = None
    ready_as_of: Optional[datetime.datetime] = None

    def is_satisfied_by(self, task: Task) -> bool:
        if self.id is not None and task.id != self.id:
            return False
        if self.state is not None and task.state != self.state:
            return False
        if self.ready_as_of is not None and not task.ready_at <= self.ready_as_of:
            return False
        return True
