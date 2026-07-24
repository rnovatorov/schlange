import dataclasses
import datetime
import traceback
import uuid
from typing import List, Optional

from schlange.internal import core as internal_core
from schlange.services.tasks import core as tasks_core

from .schedule import Schedule
from .schedule_repository import ScheduleRepository
from .schedule_specification import ScheduleSpecification


@dataclasses.dataclass
class ScheduleService:

    schedule_repository: ScheduleRepository
    task_service: tasks_core.TaskService

    def create_schedule(
        self,
        delay: float,
        interval: float,
        retry_policy: tasks_core.RetryPolicy,
        enabled: bool,
        task_args: internal_core.DTO,
        task_kind: str,
        task_retry_policy: tasks_core.RetryPolicy,
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
            task_kind=task_kind,
            task_retry_policy=task_retry_policy,
        )
        self.schedule_repository.create_schedule(schedule)
        return schedule

    def fireable_schedules(self) -> List[Schedule]:
        return self.list_schedules(
            ScheduleSpecification(
                enabled=True,
                ready_as_of=self._now(),
            )
        )

    def list_schedules(self, spec: ScheduleSpecification) -> List[Schedule]:
        return self.schedule_repository.list_schedules(spec)

    def fire_schedule(self, schedule_id: str) -> Schedule:
        schedule = self.schedule_repository.get_schedule(schedule_id)
        schedule.begin_firing(now=self._now())
        error: Optional[str] = None
        try:
            _ = self.task_service.create_task(
                id=schedule.generate_task_id(),
                args=schedule.task_args,
                kind=schedule.task_kind,
                delay=0,
                retry_policy=schedule.task_retry_policy,
                schedule_id=schedule_id,
            )
        except tasks_core.TaskAlreadyExistsError:
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
