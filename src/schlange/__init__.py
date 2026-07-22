import logging

from schlange.internal import core
from schlange.schlange import (
    DEFAULT_EXECUTION_WORKER_THREADS,
    DEFAULT_RETRY_POLICY,
    DEFAULT_SCHEDULE_DATABASE_PATH,
    DEFAULT_TASK_DATABASE_PATH,
    Schlange,
    new,
)
from schlange.services.schedule_manager import core as schedule_manager_core
from schlange.services.task_manager import core as task_manager_core

CleanupPolicy = task_manager_core.CleanupPolicy
DTO = core.DTO
RetryPolicy = task_manager_core.RetryPolicy
Schedule = schedule_manager_core.Schedule
ScheduleFiring = schedule_manager_core.ScheduleFiring
Task = task_manager_core.Task
TaskExecution = task_manager_core.TaskExecution
TaskHandler = task_manager_core.TaskHandler
TaskState = task_manager_core.TaskState

logging.getLogger(__name__).handlers = [logging.NullHandler()]

__all__ = [
    "CleanupPolicy",
    "DEFAULT_EXECUTION_WORKER_THREADS",
    "DEFAULT_RETRY_POLICY",
    "DEFAULT_SCHEDULE_DATABASE_PATH",
    "DEFAULT_TASK_DATABASE_PATH",
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
