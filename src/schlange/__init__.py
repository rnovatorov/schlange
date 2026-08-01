import logging

from schlange.internal import core
from schlange.schlange import (
    DEFAULT_RETRY_POLICY,
    DEFAULT_SCHEDULE_DATABASE_PATH,
    DEFAULT_TASK_DATABASE_PATH,
    DEFAULT_VISIBILITY_TIMEOUT,
    Schlange,
    new,
)
from schlange.services.schedules import core as schedules_core
from schlange.services.tasks import core as tasks_core

CleanupPolicy = tasks_core.CleanupPolicy
DTO = core.DTO
RetryPolicy = tasks_core.RetryPolicy
Schedule = schedules_core.Schedule
ScheduleFiring = schedules_core.ScheduleFiring
Task = tasks_core.Task
TaskExecution = tasks_core.TaskExecution
TaskHandler = tasks_core.TaskHandler
TaskState = tasks_core.TaskState

logging.getLogger(__name__).handlers = [logging.NullHandler()]

__all__ = [
    "CleanupPolicy",
    "DEFAULT_RETRY_POLICY",
    "DEFAULT_SCHEDULE_DATABASE_PATH",
    "DEFAULT_TASK_DATABASE_PATH",
    "DEFAULT_VISIBILITY_TIMEOUT",
    "DTO",
    "RetryPolicy",
    "Schedule",
    "ScheduleFiring",
    "Schlange",
    "Task",
    "TaskExecution",
    "TaskHandler",
    "TaskState",
    "new",
]
