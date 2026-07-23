from schlange.internal import core as internal_core
from schlange.internal import sqlite
from schlange.services.tasks import core


class DataMapper(sqlite.DataMapper):

    def dump_task_execution(self, execution: core.TaskExecution) -> internal_core.DTO:
        return {
            "begun_at": self.dump_timestamp(execution.begun_at),
            "ended_at": (
                self.dump_timestamp(execution.ended_at)
                if execution.ended_at is not None
                else None
            ),
            "error": execution.error if execution.error is not None else None,
        }

    def load_task_execution(self, dto: internal_core.DTO) -> core.TaskExecution:
        return core.TaskExecution(
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
