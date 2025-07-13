import logging
import queue

from .errors import (
    TaskLockedError,
    TaskNotActiveError,
    TaskNotFoundError,
    TaskNotReadyError,
)
from .task import Task
from .task_handler import TaskHandler
from .task_service import TaskService
from .worker import Worker

LOGGER = logging.getLogger(__name__)


class ExecutionWorker(Worker):

    def __init__(
        self,
        id: int,
        task_service: TaskService,
        task_handler: TaskHandler,
        queue: queue.Queue,
    ) -> None:
        super().__init__(name=f"flockq.ExecutionWorker-{id}", interval=1)
        self.task_service = task_service
        self.task_handler = task_handler
        self.queue = queue

    def work(self) -> None:
        while True:
            try:
                task = self.queue.get()
            except queue.ShutDown:
                return
            self._execute_task(task)
            self.queue.task_done()

    def _execute_task(self, task: Task) -> None:
        try:
            LOGGER.debug("executing task: id=%s", task.id)
            task = self.task_service.execute_task(
                task.id, task_handler=self.task_handler
            )
            assert task.last_execution is not None
            assert task.last_execution.duration is not None
            LOGGER.info(
                "task executed: id=%s, duration=%r, err=%r",
                task.id,
                task.last_execution.duration,
                task.last_execution.error,
            )
        except IOError as err:
            LOGGER.error("failed to execute task: id=%s, err=%r", task.id, err)
        except (
            TaskNotActiveError,
            TaskNotReadyError,
            TaskLockedError,
            TaskNotFoundError,
        ) as err:
            LOGGER.debug("failed to execute task: id=%s, err=%r", task.id, err)
