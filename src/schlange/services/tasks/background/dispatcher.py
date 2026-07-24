import logging

from schlange.internal import background
from schlange.services.tasks import core

LOGGER = logging.getLogger(__name__)


class Dispatcher(background.Worker):
    """Leader-gated worker that dispatches executable tasks."""

    def __init__(
        self,
        service: core.TaskService,
        holder: str,
        key: str,
        ttl: float,
        interval: float,
    ) -> None:
        super().__init__(name="schlange.Dispatcher", interval=interval)
        self.service = service
        self.holder = holder
        self.key = key
        self.ttl = ttl

    def work(self) -> None:
        self.dispatch_tasks()

    def dispatch_tasks(self) -> None:
        if not self.service.acquire_lease(self.key, self.holder, self.ttl):
            return
        for task in self.service.executable_tasks():
            LOGGER.debug("dispatching task: id=%s", task.id)
            try:
                self.service.begin_execution(task.id)
                LOGGER.info("dispatched task: id=%s", task.id)
            except IOError as err:
                LOGGER.error("failed to dispatch task: id=%s, err=%r", task.id, err)
            except (
                core.TaskNotFoundError,
                core.TaskNotActiveError,
                core.TaskNotReadyError,
                core.TaskExecutionNotEndedYetError,
                core.TaskUpdatedConcurrentlyError,
            ) as err:
                LOGGER.debug("failed to dispatch task: id=%s, err=%r", task.id, err)
