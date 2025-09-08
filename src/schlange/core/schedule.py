import dataclasses
import datetime
import uuid
from typing import List, Optional

from .aggregate import Aggregate
from .dto import DTO
from .errors import (
    ScheduleFiringAlreadyEndedError,
    ScheduleFiringNotBegunYetError,
    ScheduleFiringNotEndedYetError,
    ScheduleNotEnabledError,
    ScheduleNotReadyError,
    TooManyAttemptsError,
)
from .retry_policy import RetryPolicy
from .schedule_firing import ScheduleFiring


@dataclasses.dataclass
class Schedule(Aggregate):

    created_at: datetime.datetime
    ready_at: datetime.datetime
    origin: datetime.datetime
    interval: float
    retry_policy: RetryPolicy
    enabled: bool
    task_args: DTO
    task_retry_policy: RetryPolicy
    task_sequence_number: int
    firings: List[ScheduleFiring]

    @classmethod
    def create(
        cls,
        now: datetime.datetime,
        id: str,
        delay: float,
        interval: float,
        retry_policy: RetryPolicy,
        enabled: bool,
        task_args: DTO,
        task_retry_policy: RetryPolicy,
    ) -> "Schedule":
        origin = now + datetime.timedelta(seconds=delay)
        return cls(
            id=id,
            version=1,
            created_at=now,
            ready_at=origin,
            origin=origin,
            interval=interval,
            retry_policy=retry_policy,
            enabled=enabled,
            task_args=task_args,
            task_retry_policy=task_retry_policy,
            task_sequence_number=1,
            firings=[],
        )

    def ready(self, now: datetime.datetime) -> bool:
        return self.ready_at <= now

    @property
    def last_firing(self) -> Optional[ScheduleFiring]:
        return self.firings[-1] if self.firings else None

    def generate_task_id(self) -> str:
        id = f"{self.id}.{self.task_sequence_number}"
        return str(uuid.uuid5(uuid.NAMESPACE_OID, id))

    def begin_firing(self, now: datetime.datetime) -> None:
        if not self.enabled:
            raise ScheduleNotEnabledError()
        if not self.ready(now):
            raise ScheduleNotReadyError()
        if self.last_firing is not None:
            if not self.last_firing.ended:
                raise ScheduleFiringNotEndedYetError()
            if self.last_firing.task_sequence_number != self.task_sequence_number:
                self.firings = []
        self.firings.append(
            ScheduleFiring.begin(
                timestamp=now, task_sequence_number=self.task_sequence_number
            )
        )

    def end_firing(self, now: datetime.datetime, error: Optional[str]) -> None:
        if self.last_firing is None:
            raise ScheduleFiringNotBegunYetError()
        if self.last_firing.ended:
            raise ScheduleFiringAlreadyEndedError()
        self.last_firing.end(timestamp=now, error=error)
        if error is not None:
            try:
                retry_at = self._next_retry_at(now=now)
                if retry_at < self._next_firing_at():
                    self.ready_at = retry_at
                    return
            except TooManyAttemptsError:
                pass
        self.task_sequence_number += 1
        self.origin += datetime.timedelta(seconds=self.interval)
        self.ready_at = self.origin

    def _next_firing_at(self) -> datetime.datetime:
        return self.origin + datetime.timedelta(seconds=self.interval)

    def _next_retry_at(self, now: datetime.datetime) -> datetime.datetime:
        delay = self.retry_policy.delay(attempts=len(self.firings))
        return now + datetime.timedelta(seconds=delay)
