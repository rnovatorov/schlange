import logging

from .cleanup_policy import CleanupPolicy
from .errors import TaskNotFoundError
from .task_service import TaskService
from .worker import Worker

LOGGER = logging.getLogger(__name__)


class CleanupWorker(Worker):

    def __init__(
        self, interval: float, task_service: TaskService, cleanup_policy: CleanupPolicy
    ) -> None:
        super().__init__(name="flockq.CleanupWorker", interval=interval)
        self.task_service = task_service
        self.cleanup_policy = cleanup_policy

    def work(self) -> None:
        for task in self.task_service.deletable_tasks(self.cleanup_policy):
            LOGGER.debug("deleting task: id=%s", task.id)
            try:
                self.task_service.delete_task(task.id)
                LOGGER.info("deleted task: id=%s", task.id)
            except IOError as err:
                LOGGER.error("failed to delete task: id=%s, err=%r", task.id, err)
            except TaskNotFoundError as err:
                LOGGER.debug("failed to delete task: id=%s, err=%r", task.id, err)
