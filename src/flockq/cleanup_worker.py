import logging

from flockq import core

from .worker import Worker

LOGGER = logging.getLogger(__name__)


class CleanupWorker(Worker):

    def __init__(
        self,
        interval: float,
        task_service: core.TaskService,
        cleanup_policy: core.CleanupPolicy,
    ) -> None:
        super().__init__(name="flockq.CleanupWorker", interval=interval)
        self.task_service = task_service
        self.cleanup_policy = cleanup_policy

    def work(self) -> None:
        self.cleanup_succeeded_tasks()
        self.cleanup_failed_tasks()

    def cleanup_succeeded_tasks(self) -> None:
        for task in self.task_service.deletable_succeeded_tasks(self.cleanup_policy):
            LOGGER.debug("deleting succeeded task: id=%s", task.id)
            try:
                self.task_service.delete_succeeded_task(task.id)
                LOGGER.info("deleted succeeded task: id=%s", task.id)
            except IOError as err:
                LOGGER.error(
                    "failed to delete succeeded task: id=%s, err=%r", task.id, err
                )
            except core.TaskNotFoundError as err:
                LOGGER.debug(
                    "failed to delete succeeded task: id=%s, err=%r", task.id, err
                )

    def cleanup_failed_tasks(self) -> None:
        for task in self.task_service.deletable_failed_tasks(self.cleanup_policy):
            LOGGER.debug("deleting failed task: id=%s", task.id)
            try:
                self.task_service.delete_failed_task(task.id)
                LOGGER.info("deleted failed task: id=%s", task.id)
            except IOError as err:
                LOGGER.error(
                    "failed to delete failed task: id=%s, err=%r", task.id, err
                )
            except core.TaskNotFoundError as err:
                LOGGER.debug(
                    "failed to delete failed task: id=%s, err=%r", task.id, err
                )
