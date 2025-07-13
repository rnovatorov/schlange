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


class ExecutionWorkerPool(Worker):

    def __init__(
        self,
        interval: float,
        task_service: TaskService,
        task_handler: TaskHandler,
        capacity: int,
    ) -> None:
        super().__init__(name="flockq.ExecutionWorkerPool", interval=interval)
        self.task_service = task_service
        self.queue: queue.Queue = queue.Queue(maxsize=capacity)
        self.workers = [
            ExecutionWorker(
                id=worker_id,
                task_service=self.task_service,
                task_handler=task_handler,
                queue=self.queue,
            )
            for worker_id in range(capacity)
        ]

    def start(self) -> None:
        for worker in self.workers:
            worker.start()
        super().start()

    def stop(self) -> None:
        self.queue.shutdown(immediate=True)
        self.queue.join()
        for worker in self.workers:
            worker.stop()
        super().stop()

    def work(self) -> None:
        progress_made = True
        while progress_made:
            progress_made = False
            for task in self.task_service.executable_tasks():
                try:
                    # FIXME: No waiting for completion.
                    self.queue.put(task)
                    progress_made = True
                except queue.ShutDown:
                    return


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
