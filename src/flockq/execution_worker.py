import concurrent.futures
import logging
import threading

from .errors import (
    TaskHandlerNotFound,
    TaskLockedError,
    TaskNotActiveError,
    TaskNotFoundError,
    TaskNotReadyError,
)
from .task import Task
from .task_service import TaskService
from .worker import Worker

LOGGER = logging.getLogger(__name__)


class ExecutionWorker(Worker):

    def __init__(
        self, interval: float, task_service: TaskService, processes: int
    ) -> None:
        super().__init__(name="flockq.ExecutionWorker", interval=interval)
        self.task_service = task_service
        self.thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=processes)
        self.semaphore = threading.BoundedSemaphore(processes)

    def stop(self) -> None:
        self.thread_pool.shutdown(wait=True)
        super().stop()

    def work(self) -> None:
        for task in self.task_service.executable_tasks():
            self.semaphore.acquire()
            try:
                future = self.thread_pool.submit(self._execute_task, task)
            except RuntimeError:
                self.semaphore.release()
                return
            future.add_done_callback(lambda _: self.semaphore.release())

    def _execute_task(self, task: Task) -> None:
        try:
            LOGGER.debug("executing task: id=%s", task.id)
            task = self.task_service.execute_task(task.id)
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
        except TaskHandlerNotFound as err:
            LOGGER.warn("failed to execute task: id=%s, err=%r", task.id, err)
        except (
            TaskNotActiveError,
            TaskNotReadyError,
            TaskLockedError,
            TaskNotFoundError,
        ) as err:
            LOGGER.debug("failed to execute task: id=%s, err=%r", task.id, err)
