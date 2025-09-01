import contextlib
import dataclasses
import logging
import os
from typing import Generator, Optional

from . import background, core, sqlite

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

DEFAULT_SQLITE_DATABASE_SYNCHRONOUS_FULL = True


@dataclasses.dataclass
class Flockq:

    task_service: core.TaskService
    retry_policy: core.RetryPolicy
    execution_worker: background.ExecutionWorker
    cleanup_worker: background.CleanupWorker

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
    @contextlib.contextmanager
    def new(
        cls,
        url: str,
        task_handler: Optional[core.TaskHandler] = None,
        retry_policy: core.RetryPolicy = DEFAULT_RETRY_POLICY,
        execution_worker_interval: float = DEFAULT_EXECUTION_WORKER_INTERVAL,
        execution_worker_processes: int = DEFAULT_EXECUTION_WORKER_PROCESSES,
        cleanup_policy: core.CleanupPolicy = DEFAULT_CLEANUP_POLICY,
        cleanup_worker_interval: float = DEFAULT_CLEANUP_WORKER_INTERVAL,
        sqlite_database_synchronous_full: bool = DEFAULT_SQLITE_DATABASE_SYNCHRONOUS_FULL,
    ) -> Generator["Flockq"]:
        with sqlite.Database.open(
            url=url, synchronous_full=sqlite_database_synchronous_full
        ) as db:
            db.migrate()
            task_repository = sqlite.TaskRepository(db=db)
            task_service = core.TaskService(
                task_repository=task_repository, task_handler=task_handler
            )
            execution_worker = background.ExecutionWorker(
                interval=execution_worker_interval,
                task_service=task_service,
                processes=execution_worker_processes,
            )
            cleanup_worker = background.CleanupWorker(
                interval=cleanup_worker_interval,
                task_service=task_service,
                cleanup_policy=cleanup_policy,
            )
            yield cls(
                task_service=task_service,
                retry_policy=retry_policy,
                execution_worker=execution_worker,
                cleanup_worker=cleanup_worker,
            )

    def create_task(
        self,
        args: core.DTO,
        delay: float = 0.0,
        retry_policy: Optional[core.RetryPolicy] = None,
    ) -> core.Task:
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

    def task(self, task_id: str) -> core.Task:
        return self.task_service.task(task_id)
