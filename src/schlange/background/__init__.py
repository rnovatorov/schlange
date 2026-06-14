"""
Background workers that drive task processing.

All workers follow a common error handling philosophy: expected
errors (IOError, domain-specific errors) are caught and logged,
never raised. This keeps the worker thread alive across transient
failures. A worker should only crash on truly unexpected errors.
"""

from .cleanup_worker import CleanupWorker
from .execution_worker import ExecutionWorker
from .heartbeat_worker import HeartbeatWorker
from .schedule_worker import ScheduleWorker
