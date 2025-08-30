import datetime

from flockq import core

from .dto import DTO
from .file_system_task_journal_record import FileSystemTaskJournalRecord


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
    def dump_event(event: core.Event) -> DTO:
        if isinstance(event, core.TaskCreated):
            return {"task_created": FileSystemDataMapper.dump_task_created(event)}
        elif isinstance(event, core.TaskExecutionBegun):
            return {
                "task_execution_begun": FileSystemDataMapper.dump_task_execution_begun(
                    event
                )
            }
        elif isinstance(event, core.TaskExecutionEnded):
            return {
                "task_execution_ended": FileSystemDataMapper.dump_task_execution_ended(
                    event
                )
            }
        elif isinstance(event, core.TaskSucceeded):
            return {"task_succeeded": FileSystemDataMapper.dump_task_succeeded(event)}
        elif isinstance(event, core.TaskDelayed):
            return {"task_delayed": FileSystemDataMapper.dump_task_delayed(event)}
        elif isinstance(event, core.TaskFailed):
            return {"task_failed": FileSystemDataMapper.dump_task_failed(event)}
        else:
            raise TypeError(event)

    @staticmethod
    def load_event(dto: DTO) -> core.Event:
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
    def dump_task_created(event: core.TaskCreated) -> DTO:
        return {
            "timestamp": FileSystemDataMapper.dump_timestamp(event.timestamp),
            "kind": event.kind,
            "args": event.args,
            "delay": event.delay,
            "retry_policy": FileSystemDataMapper.dump_retry_policy(event.retry_policy),
        }

    @staticmethod
    def load_task_created(dto: DTO) -> core.TaskCreated:
        return core.TaskCreated(
            timestamp=FileSystemDataMapper.load_timestamp(dto["timestamp"]),
            kind=dto["kind"],
            args=dto["args"],
            delay=dto["delay"],
            retry_policy=FileSystemDataMapper.load_retry_policy(dto["retry_policy"]),
        )

    @staticmethod
    def dump_task_execution_begun(event: core.TaskExecutionBegun) -> DTO:
        return {
            "timestamp": FileSystemDataMapper.dump_timestamp(event.timestamp),
        }

    @staticmethod
    def load_task_execution_begun(dto: DTO) -> core.TaskExecutionBegun:
        return core.TaskExecutionBegun(
            timestamp=FileSystemDataMapper.load_timestamp(dto["timestamp"]),
        )

    @staticmethod
    def dump_task_execution_ended(event: core.TaskExecutionEnded) -> DTO:
        return {
            "timestamp": FileSystemDataMapper.dump_timestamp(event.timestamp),
            "error": event.error,
        }

    @staticmethod
    def load_task_execution_ended(dto: DTO) -> core.TaskExecutionEnded:
        return core.TaskExecutionEnded(
            timestamp=FileSystemDataMapper.load_timestamp(dto["timestamp"]),
            error=dto.get("error"),
        )

    @staticmethod
    def dump_task_succeeded(event: core.TaskSucceeded) -> DTO:
        return {
            "timestamp": FileSystemDataMapper.dump_timestamp(event.timestamp),
        }

    @staticmethod
    def load_task_succeeded(dto: DTO) -> core.TaskSucceeded:
        return core.TaskSucceeded(
            timestamp=FileSystemDataMapper.load_timestamp(dto["timestamp"]),
        )

    @staticmethod
    def dump_task_failed(event: core.TaskFailed) -> DTO:
        return {
            "timestamp": FileSystemDataMapper.dump_timestamp(event.timestamp),
        }

    @staticmethod
    def load_task_failed(dto: DTO) -> core.TaskFailed:
        return core.TaskFailed(
            timestamp=FileSystemDataMapper.load_timestamp(dto["timestamp"]),
        )

    @staticmethod
    def dump_task_delayed(event: core.TaskDelayed) -> DTO:
        return {
            "timestamp": FileSystemDataMapper.dump_timestamp(event.timestamp),
            "delay": event.delay,
        }

    @staticmethod
    def load_task_delayed(dto: DTO) -> core.TaskDelayed:
        return core.TaskDelayed(
            timestamp=FileSystemDataMapper.load_timestamp(dto["timestamp"]),
            delay=dto["delay"],
        )

    @staticmethod
    def dump_retry_policy(policy: core.RetryPolicy) -> DTO:
        return {
            "initial_delay": policy.initial_delay,
            "backoff_factor": policy.backoff_factor,
            "max_delay": policy.max_delay,
            "max_attempts": policy.max_attempts,
        }

    @staticmethod
    def load_retry_policy(dto: DTO) -> core.RetryPolicy:
        return core.RetryPolicy(
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
