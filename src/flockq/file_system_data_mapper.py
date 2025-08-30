import datetime

from .dto import DTO
from .event import Event
from .file_system_task_journal_record import FileSystemTaskJournalRecord
from .retry_policy import RetryPolicy
from .task_events import (
    TaskCreated,
    TaskDelayed,
    TaskExecutionBegun,
    TaskExecutionEnded,
    TaskFailed,
    TaskSucceeded,
)


class FileSystemDataMapper:

    @staticmethod
    def dump_task_journal_record(record: FileSystemTaskJournalRecord) -> DTO:
        return {
            "events": [
                FileSystemDataMapper.dump_event(event) for event in record.events
            ]
        }

    @staticmethod
    def load_task_journal_record(dto: DTO) -> FileSystemTaskJournalRecord:
        return FileSystemTaskJournalRecord(
            events=[FileSystemDataMapper.load_event(event) for event in dto["events"]]
        )

    @staticmethod
    def dump_event(event: Event) -> DTO:
        if isinstance(event, TaskCreated):
            return {"task_created": FileSystemDataMapper.dump_task_created(event)}
        elif isinstance(event, TaskExecutionBegun):
            return {
                "task_execution_begun": FileSystemDataMapper.dump_task_execution_begun(
                    event
                )
            }
        elif isinstance(event, TaskExecutionEnded):
            return {
                "task_execution_ended": FileSystemDataMapper.dump_task_execution_ended(
                    event
                )
            }
        elif isinstance(event, TaskSucceeded):
            return {"task_succeeded": FileSystemDataMapper.dump_task_succeeded(event)}
        elif isinstance(event, TaskDelayed):
            return {"task_delayed": FileSystemDataMapper.dump_task_delayed(event)}
        elif isinstance(event, TaskFailed):
            return {"task_failed": FileSystemDataMapper.dump_task_failed(event)}
        else:
            raise TypeError(event)

    @staticmethod
    def load_event(dto: DTO) -> Event:
        keys = list(dto.keys())
        assert len(keys) == 1
        event_type = keys[0]
        loader = {
            "task_created": FileSystemDataMapper.load_task_created,
            "task_execution_begun": FileSystemDataMapper.load_task_execution_begun,
            "task_execution_ended": FileSystemDataMapper.load_task_execution_ended,
            "task_succeeded": FileSystemDataMapper.load_task_succeeded,
            "task_delayed": FileSystemDataMapper.load_task_delayed,
            "task_failed": FileSystemDataMapper.load_task_failed,
        }.get(event_type)
        if loader is None:
            raise TypeError(event_type)
        return loader(dto[event_type])

    @staticmethod
    def dump_task_created(event: TaskCreated) -> DTO:
        return {
            "timestamp": FileSystemDataMapper.dump_timestamp(event.timestamp),
            "kind": event.kind,
            "args": event.args,
            "delay": event.delay,
            "retry_policy": FileSystemDataMapper.dump_retry_policy(event.retry_policy),
        }

    @staticmethod
    def load_task_created(dto: DTO) -> TaskCreated:
        return TaskCreated(
            timestamp=FileSystemDataMapper.load_timestamp(dto["timestamp"]),
            kind=dto["kind"],
            args=dto["args"],
            delay=dto["delay"],
            retry_policy=FileSystemDataMapper.load_retry_policy(dto["retry_policy"]),
        )

    @staticmethod
    def dump_task_execution_begun(event: TaskExecutionBegun) -> DTO:
        return {
            "timestamp": FileSystemDataMapper.dump_timestamp(event.timestamp),
        }

    @staticmethod
    def load_task_execution_begun(dto: DTO) -> TaskExecutionBegun:
        return TaskExecutionBegun(
            timestamp=FileSystemDataMapper.load_timestamp(dto["timestamp"]),
        )

    @staticmethod
    def dump_task_execution_ended(event: TaskExecutionEnded) -> DTO:
        return {
            "timestamp": FileSystemDataMapper.dump_timestamp(event.timestamp),
            "error": event.error,
        }

    @staticmethod
    def load_task_execution_ended(dto: DTO) -> TaskExecutionEnded:
        return TaskExecutionEnded(
            timestamp=FileSystemDataMapper.load_timestamp(dto["timestamp"]),
            error=dto.get("error"),
        )

    @staticmethod
    def dump_task_succeeded(event: TaskSucceeded) -> DTO:
        return {
            "timestamp": FileSystemDataMapper.dump_timestamp(event.timestamp),
        }

    @staticmethod
    def load_task_succeeded(dto: DTO) -> TaskSucceeded:
        return TaskSucceeded(
            timestamp=FileSystemDataMapper.load_timestamp(dto["timestamp"]),
        )

    @staticmethod
    def dump_task_failed(event: TaskFailed) -> DTO:
        return {
            "timestamp": FileSystemDataMapper.dump_timestamp(event.timestamp),
        }

    @staticmethod
    def load_task_failed(dto: DTO) -> TaskFailed:
        return TaskFailed(
            timestamp=FileSystemDataMapper.load_timestamp(dto["timestamp"]),
        )

    @staticmethod
    def dump_task_delayed(event: TaskDelayed) -> DTO:
        return {
            "timestamp": FileSystemDataMapper.dump_timestamp(event.timestamp),
            "delay": event.delay,
        }

    @staticmethod
    def load_task_delayed(dto: DTO) -> TaskDelayed:
        return TaskDelayed(
            timestamp=FileSystemDataMapper.load_timestamp(dto["timestamp"]),
            delay=dto["delay"],
        )

    @staticmethod
    def dump_retry_policy(policy: RetryPolicy) -> DTO:
        return {
            "initial_delay": policy.initial_delay,
            "backoff_factor": policy.backoff_factor,
            "max_delay": policy.max_delay,
            "max_attempts": policy.max_attempts,
        }

    @staticmethod
    def load_retry_policy(dto: DTO) -> RetryPolicy:
        return RetryPolicy(
            initial_delay=dto["initial_delay"],
            backoff_factor=dto["backoff_factor"],
            max_delay=dto.get("max_delay"),
            max_attempts=dto["max_attempts"],
        )

    @staticmethod
    def load_timestamp(s: str) -> datetime.datetime:
        return datetime.datetime.fromisoformat(s)

    @staticmethod
    def dump_timestamp(timestamp: datetime.datetime) -> str:
        return timestamp.isoformat()
