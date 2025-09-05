import dataclasses
import datetime
from typing import Optional

from .schedule import Schedule


@dataclasses.dataclass
class ScheduleSpecification:

    enabled: Optional[bool] = None
    ready_as_of: Optional[datetime.datetime] = None

    def is_satisfied_by(self, schedule: Schedule) -> bool:
        preconditions = [
            (self.enabled is None or schedule.enabled == self.enabled),
            (self.ready_as_of is None or schedule.ready_at <= self.ready_as_of),
        ]
        return all(preconditions)
