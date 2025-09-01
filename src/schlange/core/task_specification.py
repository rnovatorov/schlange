import dataclasses
import datetime
from typing import Optional

from .task import Task
from .task_state import TaskState


@dataclasses.dataclass
class TaskSpecification:

    state: Optional[TaskState] = None
    ready_as_of: Optional[datetime.datetime] = None
    last_execution_ended_before: Optional[datetime.datetime] = None

    def is_satisfied_by(self, task: Task) -> bool:
        preconditions = [
            (self.state is None or task.state == self.state),
            (self.ready_as_of is None or task.ready_at <= self.ready_as_of),
            (
                self.last_execution_ended_before is None
                or (
                    task.last_execution is not None
                    and task.last_execution.ended_at is not None
                    and task.last_execution.ended_at < self.last_execution_ended_before
                )
            ),
        ]
        return all(preconditions)
