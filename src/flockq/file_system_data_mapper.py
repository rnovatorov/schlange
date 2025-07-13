import datetime
from typing import Any, Dict

from .file_system_task_journal_record import FileSystemTaskJournalRecord
from .retry_policy import RetryPolicy
from .task_events import (
    TaskCreated,
    TaskDelayed,
    TaskEvent,
    TaskExecutionBegun,
    TaskExecutionEnded,
    TaskFailed,
    TaskSucceeded,
)


class FileSystemDataMapper:

    @staticmethod
    def dump_task_journal_record(record: FileSystemTaskJournalRecord) -> Dict[str, Any]:
        return {
            "events": [
                FileSystemDataMapper.dump_task_event(event) for event in record.events
            ]
        }

    @staticmethod
    def load_task_journal_record(dto: Dict[str, Any]) -> FileSystemTaskJournalRecord:
        return FileSystemTaskJournalRecord(
            events=[
                FileSystemDataMapper.load_task_event(event) for event in dto["events"]
            ]
        )

    @staticmethod
    def dump_task_event(event: TaskEvent) -> Dict[str, Any]:
        if isinstance(event, TaskCreated):
            return {"created": FileSystemDataMapper.dump_task_created(event)}
        elif isinstance(event, TaskExecutionBegun):
            return {
                "execution_begun": FileSystemDataMapper.dump_task_execution_begun(event)
            }
        elif isinstance(event, TaskExecutionEnded):
            return {
                "execution_ended": FileSystemDataMapper.dump_task_execution_ended(event)
            }
        elif isinstance(event, TaskSucceeded):
            return {"succeeded": FileSystemDataMapper.dump_task_succeeded(event)}
        elif isinstance(event, TaskDelayed):
            return {"delayed": FileSystemDataMapper.dump_task_delayed(event)}
        elif isinstance(event, TaskFailed):
            return {"failed": FileSystemDataMapper.dump_task_failed(event)}
        else:
            raise TypeError(event)

    @staticmethod
    def load_task_event(dto: Dict[str, Any]) -> TaskEvent:
        keys = list(dto.keys())
        assert len(keys) == 1
        event_type = keys[0]
        loader = {
            "created": FileSystemDataMapper.load_task_created,
            "execution_begun": FileSystemDataMapper.load_task_execution_begun,
            "execution_ended": FileSystemDataMapper.load_task_execution_ended,
            "succeeded": FileSystemDataMapper.load_task_succeeded,
            "delayed": FileSystemDataMapper.load_task_delayed,
            "failed": FileSystemDataMapper.load_task_failed,
        }.get(event_type)
        if loader is None:
            raise TypeError(event_type)
        return loader(dto[event_type])

    @staticmethod
    def dump_task_created(event: TaskCreated) -> Dict[str, Any]:
        return {
            "timestamp": FileSystemDataMapper.dump_timestamp(event.timestamp),
            "args": event.args,
            "delay": event.delay,
            "retry_policy": FileSystemDataMapper.dump_retry_policy(event.retry_policy),
        }

    @staticmethod
    def load_task_created(dto: Dict[str, Any]) -> TaskCreated:
        return TaskCreated(
            timestamp=FileSystemDataMapper.load_timestamp(dto["timestamp"]),
            args=dto["args"],
            delay=dto["delay"],
            retry_policy=FileSystemDataMapper.load_retry_policy(dto["retry_policy"]),
        )

    @staticmethod
    def dump_task_execution_begun(event: TaskExecutionBegun) -> Dict[str, Any]:
        return {
            "timestamp": FileSystemDataMapper.dump_timestamp(event.timestamp),
        }

    @staticmethod
    def load_task_execution_begun(dto: Dict[str, Any]) -> TaskExecutionBegun:
        return TaskExecutionBegun(
            timestamp=FileSystemDataMapper.load_timestamp(dto["timestamp"]),
        )

    @staticmethod
    def dump_task_execution_ended(event: TaskExecutionEnded) -> Dict[str, Any]:
        return {
            "timestamp": FileSystemDataMapper.dump_timestamp(event.timestamp),
            "error": event.error,
        }

    @staticmethod
    def load_task_execution_ended(dto: Dict[str, Any]) -> TaskExecutionEnded:
        return TaskExecutionEnded(
            timestamp=FileSystemDataMapper.load_timestamp(dto["timestamp"]),
            error=dto.get("error"),
        )

    @staticmethod
    def dump_task_succeeded(event: TaskSucceeded) -> Dict[str, Any]:
        return {
            "timestamp": FileSystemDataMapper.dump_timestamp(event.timestamp),
        }

    @staticmethod
    def load_task_succeeded(dto: Dict[str, Any]) -> TaskSucceeded:
        return TaskSucceeded(
            timestamp=FileSystemDataMapper.load_timestamp(dto["timestamp"]),
        )

    @staticmethod
    def dump_task_failed(event: TaskFailed) -> Dict[str, Any]:
        return {
            "timestamp": FileSystemDataMapper.dump_timestamp(event.timestamp),
        }

    @staticmethod
    def load_task_failed(dto: Dict[str, Any]) -> TaskFailed:
        return TaskFailed(
            timestamp=FileSystemDataMapper.load_timestamp(dto["timestamp"]),
        )

    @staticmethod
    def dump_task_delayed(event: TaskDelayed) -> Dict[str, Any]:
        return {
            "timestamp": FileSystemDataMapper.dump_timestamp(event.timestamp),
            "delay": event.delay,
        }

    @staticmethod
    def load_task_delayed(dto: Dict[str, Any]) -> TaskDelayed:
        return TaskDelayed(
            timestamp=FileSystemDataMapper.load_timestamp(dto["timestamp"]),
            delay=dto["delay"],
        )

    @staticmethod
    def dump_retry_policy(policy: RetryPolicy) -> Dict[str, Any]:
        return {
            "initial_delay": policy.initial_delay,
            "backoff_factor": policy.backoff_factor,
            "max_delay": policy.max_delay,
            "max_attempts": policy.max_attempts,
        }

    @staticmethod
    def load_retry_policy(dto: Dict[str, Any]) -> RetryPolicy:
        return RetryPolicy(
            initial_delay=dto["initial_delay"],
            backoff_factor=dto["backoff_factor"],
            max_delay=dto.get("max_delay"),
            max_attempts=dto["max_attempts"],
        )

    @staticmethod
    def load_timestamp(dto: str) -> datetime.datetime:
        return datetime.datetime.fromisoformat(dto)

    @staticmethod
    def dump_timestamp(timestamp: datetime.datetime) -> str:
        return timestamp.isoformat()
