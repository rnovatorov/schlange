import datetime
from typing import Any, Dict

from .retry_policy import RetryPolicy
from .task import Task
from .task_execution import TaskExecution
from .task_state import TaskState


class FileSystemDataMapper:

    @staticmethod
    def load_task(dto: Dict[str, Any]) -> Task:
        return Task(
            id=dto["id"],
            args=dto["args"],
            state=TaskState(dto["state"]),
            ready_at=datetime.datetime.fromisoformat(dto["ready_at"]),
            retry_policy=FileSystemDataMapper.load_retry_policy(dto["retry_policy"]),
            executions=[
                FileSystemDataMapper.load_task_execution(e) for e in dto["executions"]
            ],
        )

    @staticmethod
    def dump_task(task: Task) -> Dict[str, Any]:
        return {
            "id": task.id,
            "args": task.args,
            "state": task.state.value,
            "ready_at": task.ready_at.isoformat(),
            "retry_policy": FileSystemDataMapper.dump_retry_policy(task.retry_policy),
            "executions": [
                FileSystemDataMapper.dump_task_execution(e) for e in task.executions
            ],
        }

    @staticmethod
    def load_retry_policy(dto: Dict[str, Any]) -> RetryPolicy:
        return RetryPolicy(
            initial_delay=dto["initial_delay"],
            backoff_factor=dto["backoff_factor"],
            max_delay=dto.get("max_delay"),
            max_attempts=dto.get("max_attempts"),
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
    def load_task_execution(dto: Dict[str, Any]) -> TaskExecution:
        return TaskExecution(
            begun_at=datetime.datetime.fromisoformat(dto["begun_at"]),
            ended_at=(
                datetime.datetime.fromisoformat(dto["ended_at"])
                if dto.get("ended_at") is not None
                else None
            ),
            error=dto.get("error"),
        )

    @staticmethod
    def dump_task_execution(execution: TaskExecution) -> Dict[str, Any]:
        return {
            "begun_at": execution.begun_at.isoformat(),
            "ended_at": (
                execution.ended_at.isoformat()
                if execution.ended_at is not None
                else None
            ),
            "error": execution.error,
        }
