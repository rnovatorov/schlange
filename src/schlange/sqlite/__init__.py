"""
SQLite persistence adapter.

Implements the repository protocols defined in the domain layer.
"""

from .connection import Connection
from .database import Database
from .errors import NoRowsError
from .schedule_repository import ScheduleRepository
from .task_repository import TaskRepository
from .transaction import Transaction

__all__ = [
    "Connection",
    "Database",
    "NoRowsError",
    "ScheduleRepository",
    "TaskRepository",
    "Transaction",
]
