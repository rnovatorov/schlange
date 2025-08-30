import dataclasses
import logging
import os
import pathlib
from typing import Callable, Optional, Union

from flockq import core

from .cleanup_worker import CleanupWorker
from .execution_worker import ExecutionWorker
from .file_system_task_repository import FileSystemTaskRepository

LOGGER = logging.getLogger(__name__)

DEFAULT_RETRY_POLICY = core.RetryPolicy(
    initial_delay=1,
    backoff_factor=2.0,
    max_delay=60 * 60 * 24,
    max_attempts=20,
)

DEFAULT_EXECUTION_WORKER_INTERVAL = 1
DEFAULT_EXECUTION_WORKER_PROCESSES = os.cpu_count() or 4

DEFAULT_CLEANUP_POLICY = core.CleanupPolicy(
    delete_succeeded_after=60 * 60 * 24,
    delete_failed_after=60 * 60 * 24 * 7,
)
DEFAULT_CLEANUP_WORKER_INTERVAL = 60

DEFAULT_TASK_HANDLER_REGISTRY = core.TaskHandlerRegistry()


@dataclasses.dataclass
class Flockq:

    task_service: core.TaskService
    retry_policy: core.RetryPolicy
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
        task_handler_registry: core.TaskHandlerRegistry = DEFAULT_TASK_HANDLER_REGISTRY,
        retry_policy: core.RetryPolicy = DEFAULT_RETRY_POLICY,
        execution_worker_interval: float = DEFAULT_EXECUTION_WORKER_INTERVAL,
        execution_worker_processes: int = DEFAULT_EXECUTION_WORKER_PROCESSES,
        cleanup_policy: core.CleanupPolicy = DEFAULT_CLEANUP_POLICY,
        cleanup_worker_interval: float = DEFAULT_CLEANUP_WORKER_INTERVAL,
    ) -> "Flockq":
        task_repository = FileSystemTaskRepository(
            data_dir_path=pathlib.Path(data_dir_path)
        )
        task_repository.make_dirs()
        task_service = core.TaskService(
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

    def task_handler(
        self, task_kind: str
    ) -> Callable[[core.TaskHandler], core.TaskHandler]:
        def decorator(task_handler: core.TaskHandler) -> core.TaskHandler:
            self.register_task_handler(task_kind, task_handler)
            return task_handler

        return decorator

    def register_task_handler(
        self, task_kind: str, task_handler: core.TaskHandler
    ) -> None:
        self.task_service.register_task_handler(task_kind, task_handler)

    def create_task(
        self,
        kind: str,
        args: core.TaskArgs,
        delay: float = 0.0,
        retry_policy: Optional[core.RetryPolicy] = None,
    ) -> core.Task:
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

    def task(self, task_id: str) -> core.Task:
        return self.task_service.task(task_id)
