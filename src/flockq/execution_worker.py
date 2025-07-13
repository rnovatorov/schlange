import logging
import multiprocessing.pool
from typing import Optional

from .errors import (
    TaskLockedError,
    TaskNotActiveError,
    TaskNotFoundError,
    TaskNotReadyError,
)
from .task import Task
from .task_executor import TaskExecutor
from .task_service import TaskService
from .worker import Worker

LOGGER = logging.getLogger(__name__)


class ExecutionWorker(Worker):

    def __init__(
        self,
        interval: float,
        task_service: TaskService,
        executor: TaskExecutor,
        processes: Optional[int] = None,
    ) -> None:
        super().__init__(name="flockq.ExecutionWorker", interval=interval)
        self.task_service = task_service
        self.executor = executor
        self.thread_pool: Optional[multiprocessing.pool.ThreadPool] = None
        self.processes = processes

    def start(self) -> None:
        self.thread_pool = multiprocessing.pool.ThreadPool(processes=self.processes)
        super().start()

    def stop(self) -> None:
        if self.thread_pool is not None:
            self.thread_pool.close()
        super().stop()
        if self.thread_pool is not None:
            self.thread_pool.terminate()

    def work(self) -> None:
        assert self.thread_pool is not None
        progress_made = True
        while progress_made:
            tasks = self.task_service.executable_tasks()
            try:
                results = self.thread_pool.map(self._execute_task, tasks)
            except ValueError:
                if not self.stopping.is_set():
                    LOGGER.exception("failed to submit tasks for execution")
                return
            progress_made = any(results)

    def _execute_task(self, task: Task) -> bool:
        try:
            LOGGER.debug("executing task: id=%s", task.id)
            task = self.task_service.execute_task(task.id, executor=self.executor)
            assert task.last_execution is not None
            assert task.last_execution.duration is not None
            LOGGER.info(
                "task executed: id=%s, duration=%r, err=%r",
                task.id,
                task.last_execution.duration,
                task.last_execution.error,
            )
            return True
        except IOError as err:
            LOGGER.error("failed to execute task: id=%s, err=%r", task.id, err)
            return False
        except (
            TaskNotActiveError,
            TaskNotReadyError,
            TaskLockedError,
            TaskNotFoundError,
        ) as err:
            LOGGER.debug("failed to execute task: id=%s, err=%r", task.id, err)
            return False
