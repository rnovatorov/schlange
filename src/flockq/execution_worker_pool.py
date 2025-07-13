import queue

from .execution_worker import ExecutionWorker
from .task_handler import TaskHandler
from .task_service import TaskService
from .worker import Worker


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
