import logging

from schlange.internal import background
from schlange.services.schedules import core

LOGGER = logging.getLogger(__name__)


class ScheduleWorker(background.Worker):
    """Leader-gated worker that fires fireable schedules."""

    def __init__(
        self,
        schedule_service: core.ScheduleService,
        holder: str,
        key: str,
        ttl: float,
        interval: float,
    ) -> None:
        super().__init__(name="schlange.ScheduleWorker", interval=interval)
        self.schedule_service = schedule_service
        self.holder = holder
        self.key = key
        self.ttl = ttl

    def work(self) -> None:
        if not self.schedule_service.acquire_lease(self.key, self.holder, self.ttl):
            return
        while True:
            schedules = self.schedule_service.fireable_schedules()
            if not schedules or self.stopping.is_set():
                return
            for schedule in schedules:
                self._fire_schedule(schedule)

    def _fire_schedule(self, schedule: core.Schedule) -> None:
        try:
            LOGGER.debug("firing schedule: id=%s", schedule.id)
            schedule = self.schedule_service.fire_schedule(schedule.id)
            assert schedule.last_firing is not None
            assert schedule.last_firing.duration is not None
            LOGGER.info(
                "fired schedule: id=%s, duration=%r, err=%r",
                schedule.id,
                schedule.last_firing.duration,
                schedule.last_firing.error,
            )
        except (
            IOError,
            core.ScheduleFiringNotEndedYetError,
            core.ScheduleFiringNotBegunYetError,
        ) as err:
            LOGGER.error("failed to fire schedule: id=%s, err=%r", schedule.id, err)
        except (
            core.ScheduleNotFoundError,
            core.ScheduleNotEnabledError,
            core.ScheduleNotReadyError,
            core.ScheduleUpdatedConcurrentlyError,
        ) as err:
            LOGGER.debug("failed to fire schedule: id=%s, err=%r", schedule.id, err)
