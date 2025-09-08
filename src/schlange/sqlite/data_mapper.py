import datetime

from schlange import core


class DataMapper:

    def dump_retry_policy(self, policy: core.RetryPolicy) -> core.DTO:
        return {
            "initial_delay": policy.initial_delay,
            "backoff_factor": policy.backoff_factor,
            "max_delay": policy.max_delay,
            "max_attempts": policy.max_attempts,
        }

    def load_retry_policy(self, dto: core.DTO) -> core.RetryPolicy:
        return core.RetryPolicy(
            initial_delay=dto["initial_delay"],
            backoff_factor=dto["backoff_factor"],
            max_delay=dto.get("max_delay"),
            max_attempts=dto["max_attempts"],
        )

    def dump_task_execution(self, execution: core.TaskExecution) -> core.DTO:
        return {
            "begun_at": self.dump_timestamp(execution.begun_at),
            "ended_at": (
                self.dump_timestamp(execution.ended_at)
                if execution.ended_at is not None
                else None
            ),
            "error": execution.error if execution.error is not None else None,
        }

    def load_task_execution(self, dto: core.DTO) -> core.TaskExecution:
        return core.TaskExecution(
            begun_at=self.load_timestamp(dto["begun_at"]),
            ended_at=(
                self.load_timestamp(dto["ended_at"])
                if dto.get("ended_at") is not None
                else None
            ),
            error=dto["error"] if dto.get("error") is not None else None,
        )

    def dump_schedule_firing(self, creation: core.ScheduleFiring) -> core.DTO:
        return {
            "task_sequence_number": creation.task_sequence_number,
            "begun_at": self.dump_timestamp(creation.begun_at),
            "ended_at": (
                self.dump_timestamp(creation.ended_at)
                if creation.ended_at is not None
                else None
            ),
            "error": creation.error if creation.error is not None else None,
        }

    def load_schedule_firing(self, dto: core.DTO) -> core.ScheduleFiring:
        return core.ScheduleFiring(
            task_sequence_number=dto["task_sequence_number"],
            begun_at=self.load_timestamp(dto["begun_at"]),
            ended_at=(
                self.load_timestamp(dto["ended_at"])
                if dto.get("ended_at") is not None
                else None
            ),
            error=dto["error"] if dto.get("error") is not None else None,
        )

    def load_task_state(self, s: str) -> core.TaskState:
        return core.TaskState(s)

    def dump_task_state(self, state: core.TaskState) -> str:
        return state.value

    def load_timestamp(self, s: str) -> datetime.datetime:
        return datetime.datetime.fromisoformat(s)

    def dump_timestamp(self, timestamp: datetime.datetime) -> str:
        return timestamp.isoformat()
