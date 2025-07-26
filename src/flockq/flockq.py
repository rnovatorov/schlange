import dataclasses
import logging
import os
import pathlib
from typing import Callable, Optional, Union

from .cleanup_policy import CleanupPolicy
from .cleanup_worker import CleanupWorker
from .execution_worker import ExecutionWorker
from .file_system_task_repository import FileSystemTaskRepository
from .retry_policy import RetryPolicy
from .task import Task
from .task_args import TaskArgs
from .task_handler import TaskHandler
from .task_handler_registry import TaskHandlerRegistry
from .task_service import TaskService

LOGGER = logging.getLogger(__name__)

DEFAULT_RETRY_POLICY = RetryPolicy(
    initial_delay=1,
    backoff_factor=2.0,
    max_delay=60 * 60 * 24,
    max_attempts=20,
)

DEFAULT_EXECUTION_WORKER_INTERVAL = 1
DEFAULT_EXECUTION_WORKER_PROCESSES = os.cpu_count() or 4

DEFAULT_CLEANUP_POLICY = CleanupPolicy(
    delete_succeeded_after=60,
    delete_failed_after=60,
)
DEFAULT_CLEANUP_WORKER_INTERVAL = 60

DEFAULT_TASK_HANDLER_REGISTRY = TaskHandlerRegistry()


@dataclasses.dataclass
class Flockq:

    task_service: TaskService
    retry_policy: RetryPolicy
    execution_worker: ExecutionWorker
    cleanup_worker: CleanupWorker

    def __enter__(self) -> "Flockq":
        self.start()
        return self

    def __exit__(self, *exc) -> None:
        self.stop()

    def start(self) -> None:
        self.execution_worker.start()
        self.cleanup_worker.start()

    def stop(self) -> None:
        self.cleanup_worker.stop()
        self.execution_worker.stop()

    @classmethod
    def new(
        cls,
        data_dir_path: Union[str, pathlib.Path],
        task_handler_registry: TaskHandlerRegistry = DEFAULT_TASK_HANDLER_REGISTRY,
        retry_policy: RetryPolicy = DEFAULT_RETRY_POLICY,
        execution_worker_interval: float = DEFAULT_EXECUTION_WORKER_INTERVAL,
        execution_worker_processes: int = DEFAULT_EXECUTION_WORKER_PROCESSES,
        cleanup_policy: CleanupPolicy = DEFAULT_CLEANUP_POLICY,
        cleanup_worker_interval: float = DEFAULT_CLEANUP_WORKER_INTERVAL,
    ) -> "Flockq":
        task_repository = FileSystemTaskRepository(
            data_dir_path=pathlib.Path(data_dir_path)
        )
        task_repository.make_dirs()
        task_service = TaskService(
            task_repository=task_repository, task_handler_registry=task_handler_registry
        )
        execution_worker = ExecutionWorker(
            interval=execution_worker_interval,
            task_service=task_service,
            processes=execution_worker_processes,
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

    def task_handler(self, task_kind: str) -> Callable[[TaskHandler], TaskHandler]:
        def decorator(task_handler: TaskHandler) -> TaskHandler:
            self.register_task_handler(task_kind, task_handler)
            return task_handler

        return decorator

    def register_task_handler(self, task_kind: str, task_handler: TaskHandler) -> None:
        self.task_service.register_task_handler(task_kind, task_handler)

    def create_task(
        self,
        kind: str,
        args: TaskArgs,
        delay: float = 0.0,
        retry_policy: Optional[RetryPolicy] = None,
    ) -> Task:
        if retry_policy is None:
            retry_policy = self.retry_policy
        LOGGER.debug(
            "creating task: kind=%s, args=%s, delay=%f, retry_policy=%r",
            kind,
            args,
            delay,
            retry_policy,
        )
        task = self.task_service.create_task(
            kind=kind, args=args, delay=delay, retry_policy=retry_policy
        )
        LOGGER.info("task created: task=%r", task)
        return task

    def task(self, task_id: str) -> Task:
        return self.task_service.task(task_id)
