import dataclasses
import datetime
import traceback
import uuid
from typing import List, Optional

from .dto import DTO
from .errors import TaskAlreadyExistsError
from .retry_policy import RetryPolicy
from .schedule import Schedule
from .schedule_repository import ScheduleRepository
from .schedule_specification import ScheduleSpecification
from .task_service import TaskService


@dataclasses.dataclass
class ScheduleService:

    schedule_repository: ScheduleRepository
    task_service: TaskService

    def create_schedule(
        self,
        delay: float,
        interval: float,
        retry_policy: RetryPolicy,
        enabled: bool,
        task_args: DTO,
        task_retry_policy: RetryPolicy,
        id: Optional[str] = None,
    ) -> Schedule:
        if id is None:
            id = str(uuid.uuid4())
        schedule = Schedule.create(
            now=self._now(),
            id=id,
            delay=delay,
            interval=interval,
            retry_policy=retry_policy,
            enabled=enabled,
            task_args=task_args,
            task_retry_policy=task_retry_policy,
        )
        self.schedule_repository.create_schedule(schedule)
        return schedule

    def fireable_schedules(self) -> List[Schedule]:
        return self.schedule_repository.list_schedules(
            ScheduleSpecification(
                enabled=True,
                ready_as_of=self._now(),
            )
        )

    def fire_schedule(self, schedule_id: str) -> Schedule:
        schedule = self.schedule_repository.get_schedule(schedule_id)
        schedule.begin_firing(now=self._now())
        error: Optional[str] = None
        try:
            _ = self.task_service.create_task(
                id=schedule.generate_task_id(),
                args=schedule.task_args,
                delay=0,
                retry_policy=schedule.task_retry_policy,
                schedule_id=schedule_id,
            )
        except TaskAlreadyExistsError:
            pass
        except Exception:
            error = traceback.format_exc()
        schedule.end_firing(now=self._now(), error=error)
        self.schedule_repository.update_schedule(schedule, synchronous=False)
        return schedule

    def schedule(self, schedule_id: str) -> Schedule:
        return self.schedule_repository.get_schedule(schedule_id)

    def delete_schedule(self, schedule_id: str) -> None:
        self.schedule_repository.delete_schedule(schedule_id)

    def _now(self) -> datetime.datetime:
        return datetime.datetime.now(datetime.UTC)
