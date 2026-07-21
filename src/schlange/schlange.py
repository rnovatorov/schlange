import contextlib
import dataclasses
import logging
import os
import pathlib
from typing import Generator, List, Optional

from schlange.internal import core, sqlite
from schlange.services.schedule_manager import background as schedule_manager_background
from schlange.services.schedule_manager import core as schedule_manager_core
from schlange.services.schedule_manager import sqlite as schedule_manager_sqlite
from schlange.services.task_executor import background as task_executor_background
from schlange.services.task_manager import background as task_manager_background
from schlange.services.task_manager import core as task_manager_core
from schlange.services.task_manager import sqlite as task_manager_sqlite

LOGGER = logging.getLogger(__name__)

DEFAULT_DATABASE_PATH = pathlib.Path("schlange.db")
DEFAULT_RETRY_POLICY = task_manager_core.RetryPolicy(
    initial_delay=1,
    backoff_factor=2.0,
    max_delay=60 * 60 * 24,
    max_attempts=20,
)

DEFAULT_EXECUTION_WORKER_INTERVAL = 1
DEFAULT_EXECUTION_WORKER_THREADS = os.cpu_count() or 4

DEFAULT_CLEANUP_POLICY = task_manager_core.CleanupPolicy(
    delete_succeeded_after=60 * 60 * 24,
    delete_failed_after=60 * 60 * 24 * 7,
)
DEFAULT_CLEANUP_WORKER_INTERVAL = 60

DEFAULT_SCHEDULE_WORKER_INTERVAL = 1


@dataclasses.dataclass
class Schlange:

    task_service: task_manager_core.TaskService
    default_retry_policy: task_manager_core.RetryPolicy
    schedule_service: schedule_manager_core.ScheduleService
    execution_worker: task_executor_background.ExecutionWorker
    cleanup_worker: task_manager_background.CleanupWorker
    schedule_worker: schedule_manager_background.ScheduleWorker

    def __enter__(self) -> "Schlange":
        self.start()
        return self

    def __exit__(self, *exc) -> None:
        self.stop()

    def start(self) -> None:
        self.execution_worker.start()
        self.cleanup_worker.start()
        self.schedule_worker.start()

    def stop(self) -> None:
        self.cleanup_worker.stop()
        self.execution_worker.stop()
        self.schedule_worker.stop()

    @classmethod
    @contextlib.contextmanager
    def new(
        cls,
        database_path: pathlib.Path = DEFAULT_DATABASE_PATH,
        task_handler: Optional[task_manager_core.TaskHandler] = None,
        default_retry_policy: task_manager_core.RetryPolicy = DEFAULT_RETRY_POLICY,
        execution_worker_interval: float = DEFAULT_EXECUTION_WORKER_INTERVAL,
        execution_worker_threads: int = DEFAULT_EXECUTION_WORKER_THREADS,
        cleanup_policy: task_manager_core.CleanupPolicy = DEFAULT_CLEANUP_POLICY,
        cleanup_worker_interval: float = DEFAULT_CLEANUP_WORKER_INTERVAL,
        schedule_worker_interval: float = DEFAULT_SCHEDULE_WORKER_INTERVAL,
    ) -> Generator["Schlange", None, None]:
        with sqlite.Database.open(
            path=database_path,
            read_pool_capacity=calculate_optimal_database_read_pool_capacity(
                execution_worker_threads
            ),
        ) as db:
            db.migrate()
            task_repository = task_manager_sqlite.TaskRepository(db=db)
            task_service = task_manager_core.TaskService(
                task_repository=task_repository, task_handler=task_handler
            )
            schedule_repository = schedule_manager_sqlite.ScheduleRepository(db=db)
            schedule_service = schedule_manager_core.ScheduleService(
                schedule_repository=schedule_repository,
                task_service=task_service,
            )
            execution_worker = task_executor_background.ExecutionWorker(
                interval=execution_worker_interval,
                task_service=task_service,
                threads=execution_worker_threads,
            )
            cleanup_worker = task_manager_background.CleanupWorker(
                interval=cleanup_worker_interval,
                task_service=task_service,
                cleanup_policy=cleanup_policy,
            )
            schedule_worker = schedule_manager_background.ScheduleWorker(
                interval=schedule_worker_interval,
                schedule_service=schedule_service,
            )
            yield cls(
                task_service=task_service,
                default_retry_policy=default_retry_policy,
                schedule_service=schedule_service,
                execution_worker=execution_worker,
                cleanup_worker=cleanup_worker,
                schedule_worker=schedule_worker,
            )

    def create_task(
        self,
        args: core.DTO,
        delay: float = 0.0,
        retry_policy: Optional[task_manager_core.RetryPolicy] = None,
        id: Optional[str] = None,
    ) -> task_manager_core.Task:
        if retry_policy is None:
            retry_policy = self.default_retry_policy
        LOGGER.debug(
            "creating task: args=%s, delay=%f, retry_policy=%r",
            args,
            delay,
            retry_policy,
        )
        task = self.task_service.create_task(
            args=args,
            delay=delay,
            retry_policy=retry_policy,
            id=id,
        )
        LOGGER.info("task created: task=%r", task)
        return task

    def task(self, task_id: str) -> task_manager_core.Task:
        return self.task_service.task(task_id)

    def delete_task(self, task_id: str) -> None:
        self.task_service.delete_task(task_id)

    def tasks(
        self, state: Optional[task_manager_core.TaskState] = None
    ) -> List[task_manager_core.Task]:
        spec = task_manager_core.TaskSpecification(state=state)
        return self.task_service.list_tasks(spec=spec)

    def reactivate_task(self, task_id: str, delay: float = 0) -> task_manager_core.Task:
        return self.task_service.reactivate_task(task_id=task_id, delay=delay)

    def create_schedule(
        self,
        task_args: core.DTO,
        interval: float,
        enabled: bool = True,
        delay: float = 0.0,
        retry_policy: Optional[task_manager_core.RetryPolicy] = None,
        task_retry_policy: Optional[task_manager_core.RetryPolicy] = None,
        id: Optional[str] = None,
    ) -> schedule_manager_core.Schedule:
        if retry_policy is None:
            retry_policy = self.default_retry_policy
        if task_retry_policy is None:
            task_retry_policy = self.default_retry_policy
        LOGGER.debug(
            "creating schedule: interval=%f, enabled=%s, task_args=%r",
            interval,
            enabled,
            task_args,
        )
        schedule = self.schedule_service.create_schedule(
            delay=delay,
            interval=interval,
            retry_policy=retry_policy,
            enabled=enabled,
            task_args=task_args,
            task_retry_policy=task_retry_policy,
            id=id,
        )
        LOGGER.info("schedule created: schedule=%r", schedule)
        return schedule

    def schedule(self, schedule_id: str) -> schedule_manager_core.Schedule:
        return self.schedule_service.schedule(schedule_id)

    def schedules(
        self, enabled: Optional[bool] = None
    ) -> List[schedule_manager_core.Schedule]:
        return self.schedule_service.list_schedules(
            schedule_manager_core.ScheduleSpecification(enabled=enabled)
        )

    def delete_schedule(self, schedule_id: str) -> None:
        self.schedule_service.delete_schedule(schedule_id)


new = Schlange.new


def calculate_optimal_database_read_pool_capacity(execution_worker_threads: int) -> int:
    execution_worker = 1
    schedule_worker = 1
    cleanup_worker = 1
    additional_capacity = 1
    return (
        execution_worker
        + execution_worker_threads
        + cleanup_worker
        + schedule_worker
        + additional_capacity
    )
