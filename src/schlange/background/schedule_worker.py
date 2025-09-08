import logging

from schlange import core

from .worker import Worker

LOGGER = logging.getLogger(__name__)


class ScheduleWorker(Worker):

    def __init__(self, interval: float, schedule_service: core.ScheduleService) -> None:
        super().__init__(name="schlange.ScheduleWorker", interval=interval)
        self.schedule_service = schedule_service

    def work(self) -> None:
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
            core.ScheduleFiringAlreadyEndedError,
        ) as err:
            LOGGER.error("failed to fire schedule: id=%s, err=%r", schedule.id, err)
        except (
            core.ScheduleNotFoundError,
            core.ScheduleNotEnabledError,
            core.ScheduleNotReadyError,
            core.ScheduleUpdatedConcurrentlyError,
        ) as err:
            LOGGER.debug("failed to fire schedule: id=%s, err=%r", schedule.id, err)
