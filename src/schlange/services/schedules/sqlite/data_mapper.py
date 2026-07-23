from schlange.internal import core as internal_core
from schlange.internal import sqlite
from schlange.services.schedules import core


class DataMapper(sqlite.DataMapper):

    def dump_schedule_firing(self, creation: core.ScheduleFiring) -> internal_core.DTO:
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

    def load_schedule_firing(self, dto: internal_core.DTO) -> core.ScheduleFiring:
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
