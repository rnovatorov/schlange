import concurrent.futures
import logging
import threading
from typing import Set

from schlange import core

from .worker import Worker

LOGGER = logging.getLogger(__name__)


class ExecutionWorker(Worker):

    def __init__(
        self, interval: float, task_service: core.TaskService, processes: int
    ) -> None:
        super().__init__(name="schlange.ExecutionWorker", interval=interval)
        self.task_service = task_service
        self.lock = threading.Lock()
        self.executing_tasks: Set[str] = set()
        self.thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=processes)

    def stop(self) -> None:
        self.thread_pool.shutdown(wait=True, cancel_futures=True)
        super().stop()

    def work(self) -> None:
        while True:
            tasks = self.task_service.executable_tasks()
            submitted = 0
            for task in tasks:
                submitted += self._submit_task(task)
            if not submitted:
                return

    def _submit_task(self, task: core.Task) -> bool:
        with self.lock:
            if task.id in self.executing_tasks:
                return False
            self.executing_tasks.add(task.id)

        def callback():
            with self.lock:
                self.executing_tasks.remove(task.id)

        try:
            future = self.thread_pool.submit(self._execute_task, task.id)
        except RuntimeError:
            callback()
            return False
        future.add_done_callback(lambda _: callback)
        return True

    def _execute_task(self, task_id: str) -> None:
        try:
            LOGGER.debug("executing task: id=%s", task_id)
            task = self.task_service.execute_task(task_id)
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
        except core.TaskHandlerNotFound as err:
            LOGGER.warning("failed to execute task: id=%s, err=%r", task.id, err)
        except (
            core.TaskNotActiveError,
            core.TaskNotReadyError,
            core.TaskUpdatedConcurrentlyError,
            core.TaskNotFoundError,
        ) as err:
            LOGGER.debug("failed to execute task: id=%s, err=%r", task.id, err)
