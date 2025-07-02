import multiprocessing.pool
from typing import Optional

from .queue import Queue
from .task import Task
from .task_executor import TaskExecutor
from .worker import Worker


class ExecutionWorker(Worker):

    def __init__(
        self,
        interval: float,
        queue: Queue,
        executor: TaskExecutor,
        processes: Optional[int] = None,
    ) -> None:
        super().__init__(name="flockq.execution_worker", interval=interval)
        self.queue = queue
        self.executor = executor
        self.thread_pool: Optional[multiprocessing.pool.ThreadPool] = None
        self.processes = processes

    def start(self) -> None:
        self.thread_pool = multiprocessing.pool.ThreadPool(processes=self.processes)
        super().start()

    def stop(self) -> None:
        if self.thread_pool is not None:
            self.thread_pool.terminate()
        super().stop()

    def work(self) -> None:
        assert self.thread_pool is not None
        progress_made = True
        while progress_made:
            tasks = self.queue.find_executable_tasks()
            progress_made = any(
                self.thread_pool.imap_unordered(self._execute_task, tasks)
            )

    def _execute_task(self, task: Task) -> bool:
        return self.queue.execute_task(task.id, executor=self.executor)
