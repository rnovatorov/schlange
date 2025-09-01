import logging

from schlange import core

from .worker import Worker

LOGGER = logging.getLogger(__name__)


class CleanupWorker(Worker):

    def __init__(
        self,
        interval: float,
        task_service: core.TaskService,
        cleanup_policy: core.CleanupPolicy,
    ) -> None:
        super().__init__(name="schlange.CleanupWorker", interval=interval)
        self.task_service = task_service
        self.cleanup_policy = cleanup_policy

    def work(self) -> None:
        self.cleanup_tasks()

    def cleanup_tasks(self) -> None:
        for task in self.task_service.deletable_tasks(self.cleanup_policy):
            LOGGER.debug("deleting task: id=%s", task.id)
            try:
                self.task_service.delete_task(task.id)
                LOGGER.info("deleted task: id=%s", task.id)
            except IOError as err:
                LOGGER.error("failed to delete task: id=%s, err=%r", task.id, err)
            except core.TaskNotFoundError as err:
                LOGGER.debug("failed to delete task: id=%s, err=%r", task.id, err)
