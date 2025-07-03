import dataclasses
import os
import pathlib
from typing import Optional, Union

from .cleanup_policy import CleanupPolicy
from .cleanup_worker import CleanupWorker
from .execution_worker import ExecutionWorker
from .file_system_task_repository import FileSystemTaskRepository
from .queue import Queue
from .retry_policy import RetryPolicy
from .task import Task
from .task_args import TaskArgs
from .task_executor import TaskExecutor


@dataclasses.dataclass
class Client:

    queue: Queue
    execution_worker: Optional[ExecutionWorker]
    cleanup_worker: CleanupWorker

    def __enter__(self) -> "Client":
        self.start()
        return self

    def __exit__(self, *exc) -> None:
        self.stop()

    def start(self) -> None:
        if self.execution_worker is not None:
            self.execution_worker.start()
        self.cleanup_worker.start()

    def stop(self) -> None:
        self.cleanup_worker.stop()
        if self.execution_worker is not None:
            self.execution_worker.stop()

    @classmethod
    def new(
        cls,
        data_dir_path: Union[str, pathlib.Path],
        executor: Optional[TaskExecutor],
        retry_policy: RetryPolicy = RetryPolicy(
            initial_delay=1,
            backoff_factor=2.0,
            max_delay=60 * 60 * 24,
            max_attempts=20,
        ),
        execution_worker_interval: float = 1,
        cleanup_policy: CleanupPolicy = CleanupPolicy(
            delete_succeeded_after=60 * 60 * 24,
            delete_failed_after=60 * 60 * 24 * 7,
        ),
        cleanup_worker_interval: float = 60,
    ) -> "Client":
        if isinstance(data_dir_path, str):
            data_dir_path = pathlib.Path(data_dir_path)
        try:
            os.mkdir(data_dir_path)
        except FileExistsError:
            pass
        task_repository = FileSystemTaskRepository(data_dir_path=data_dir_path)
        queue = Queue(
            task_repository=task_repository,
            retry_policy=retry_policy,
            cleanup_policy=cleanup_policy,
        )
        execution_worker = (
            ExecutionWorker(
                interval=execution_worker_interval, queue=queue, executor=executor
            )
            if executor is not None
            else None
        )
        cleanup_worker = CleanupWorker(interval=cleanup_worker_interval, queue=queue)
        return cls(
            queue=queue,
            execution_worker=execution_worker,
            cleanup_worker=cleanup_worker,
        )

    def create_task(
        self,
        args: TaskArgs,
        delay: float = 0.0,
        retry_policy: Optional[RetryPolicy] = None,
    ) -> Task:
        return self.queue.create_task(args=args, delay=delay, retry_policy=retry_policy)

    def find_task(self, task_id: str) -> Optional[Task]:
        return self.queue.find_task(task_id)
