import dataclasses
import logging
import os
import pathlib
from typing import Optional, Union

from .cleanup_policy import CleanupPolicy
from .cleanup_worker import CleanupWorker
from .execution_worker import ExecutionWorker
from .file_system_task_repository import FileSystemTaskRepository
from .retry_policy import RetryPolicy
from .task import Task
from .task_args import TaskArgs
from .task_executor import TaskExecutor
from .task_service import TaskService

LOGGER = logging.getLogger(__name__)

DEFAULT_RETRY_POLICY = RetryPolicy(
    initial_delay=1,
    backoff_factor=2.0,
    max_delay=60 * 60 * 24,
    max_attempts=20,
)

DEFAULT_CLEANUP_POLICY = CleanupPolicy(
    delete_succeeded_after=60,
    delete_failed_after=60,
)


@dataclasses.dataclass
class Flockq:

    task_service: TaskService
    retry_policy: RetryPolicy
    execution_worker: Optional[ExecutionWorker]
    cleanup_worker: CleanupWorker

    def __enter__(self) -> "Flockq":
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
        retry_policy: RetryPolicy = DEFAULT_RETRY_POLICY,
        execution_worker_interval: float = 1,
        cleanup_policy: CleanupPolicy = DEFAULT_CLEANUP_POLICY,
        cleanup_worker_interval: float = 60,
    ) -> "Flockq":
        if isinstance(data_dir_path, str):
            data_dir_path = pathlib.Path(data_dir_path)
        try:
            os.mkdir(data_dir_path)
        except FileExistsError:
            pass
        task_repository = FileSystemTaskRepository(data_dir_path=data_dir_path)
        task_service = TaskService(task_repository=task_repository)
        execution_worker = (
            ExecutionWorker(
                interval=execution_worker_interval,
                task_service=task_service,
                executor=executor,
            )
            if executor is not None
            else None
        )
        cleanup_worker = CleanupWorker(
            interval=cleanup_worker_interval,
            task_service=task_service,
            cleanup_policy=cleanup_policy,
        )
        return cls(
            task_service=task_service,
            retry_policy=retry_policy,
            execution_worker=execution_worker,
            cleanup_worker=cleanup_worker,
        )

    def create_task(
        self,
        args: TaskArgs,
        delay: float = 0.0,
        retry_policy: Optional[RetryPolicy] = None,
    ) -> Task:
        if retry_policy is None:
            retry_policy = self.retry_policy
        LOGGER.debug(
            "creating task: args=%s, delay=%f, retry_policy=%r",
            args,
            delay,
            retry_policy,
        )
        task = self.task_service.create_task(
            args=args, delay=delay, retry_policy=retry_policy
        )
        LOGGER.info("task created: task=%r", task)
        return task

    def task(self, task_id: str) -> Task:
        return self.task_service.task(task_id)
