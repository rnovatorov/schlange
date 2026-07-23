from typing import List, Protocol

from .schedule import Schedule
from .schedule_specification import ScheduleSpecification


class ScheduleRepository(Protocol):

    def create_schedule(self, schedule: Schedule) -> None:
        pass

    def list_schedules(self, spec: ScheduleSpecification) -> List[Schedule]:
        pass

    def get_schedule(self, schedule_id: str) -> Schedule:
        pass

    def delete_schedule(self, schedule_id: str) -> None:
        pass

    def update_schedule(self, schedule: Schedule, synchronous: bool) -> None:
        pass
