import logging

from .errors import TaskNotFoundError
from .queue import Queue
from .worker import Worker

LOGGER = logging.getLogger(__name__)


class CleanupWorker(Worker):

    def __init__(self, interval: float, queue: Queue) -> None:
        super().__init__(name="flockq.CleanupWorker", interval=interval)
        self.queue = queue

    def work(self) -> None:
        for task in self.queue.find_deletable_tasks():
            LOGGER.debug("deleting task: id=%s", task.id)
            try:
                self.queue.delete_task(task.id)
                LOGGER.info("deleted task: id=%s", task.id)
            except IOError as err:
                LOGGER.error("failed to delete task: id=%s, err=%r", task.id, err)
            except TaskNotFoundError as err:
                LOGGER.debug("failed to delete task: id=%s, err=%r", task.id, err)
