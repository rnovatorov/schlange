import datetime

from .cleanup_policy import CleanupPolicy
from .specification import Specification
from .task import Task
from .task_state import TaskState


class TaskSpecification(Specification[Task]):
    pass


class TaskIsInState(TaskSpecification):

    def __init__(self, state: TaskState) -> None:
        self.state = state

    def is_satisfied_by(self, task: Task) -> bool:
        return task.state == self.state


class TaskIsReady(TaskSpecification):

    def __init__(self, now: datetime.datetime) -> None:
        self.now = now

    def is_satisfied_by(self, task: Task) -> bool:
        return task.ready_at <= self.now


class LastTaskExecutionHasEndedBefore(TaskSpecification):

    def __init__(self, timestamp: datetime.datetime) -> None:
        self.timestamp = timestamp

    def is_satisfied_by(self, task: Task) -> bool:
        return (
            task.last_execution is not None
            and task.last_execution.ended_at is not None
            and task.last_execution.ended_at < self.timestamp
        )


class TaskIsExecutable(TaskSpecification):

    def __init__(self, now: datetime.datetime) -> None:
        self.now = now

    def is_satisfied_by(self, task: Task) -> bool:
        return (
            TaskIsInState(TaskState.ACTIVE) & TaskIsReady(self.now)
        ).is_satisfied_by(task)


class TaskIsDeletable(TaskSpecification):

    def __init__(self, now: datetime.datetime, cleanup_policy: CleanupPolicy) -> None:
        self.now = now
        self.cleanup_policy = cleanup_policy

    def is_satisfied_by(self, task: Task) -> bool:
        succeeded_deadline = self.cleanup_policy.succeeded_deadline(self.now)
        failed_deadline = self.cleanup_policy.failed_deadline(self.now)
        return (
            (
                TaskIsInState(TaskState.SUCCEEDED)
                & LastTaskExecutionHasEndedBefore(succeeded_deadline)
            )
            | (
                TaskIsInState(TaskState.FAILED)
                & LastTaskExecutionHasEndedBefore(failed_deadline)
            )
        ).is_satisfied_by(task)


# @dataclasses.dataclass
# class TaskSpecificationOld:
#
#     id: Optional[str] = None
#     state: Optional[TaskState] = None
#     ready_as_of: Optional[datetime.datetime] = None
#     last_execution_ended_before: Optional[datetime.datetime] = None
#
#     def is_satisfied_by(self, task: Task) -> bool:
#         if self.id is not None and task.id != self.id:
#             return False
#         if self.state is not None and task.state != self.state:
#             return False
#         if self.ready_as_of is not None and not task.ready_as_of(self.ready_as_of):
#             return False
#         return True
