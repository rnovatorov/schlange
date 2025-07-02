from .queue import Queue
from .worker import Worker


class CleanupWorker(Worker):

    def __init__(self, queue: Queue, interval: float) -> None:
        super().__init__(name="flockueue.cleanup_worker", interval=interval)
        self.queue = queue

    def work(self) -> None:
        for task in self.queue.find_deletable_tasks():
            _ = self.queue.delete_task(task.id)
