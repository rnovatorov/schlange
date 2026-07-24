import contextlib
import dataclasses
import logging
import os
import pathlib
from typing import Generator, List, Optional

from schlange.internal import core, sqlite
from schlange.services.execution import background as execution_background
from schlange.services.schedules import background as schedules_background
from schlange.services.schedules import core as schedules_core
from schlange.services.schedules import sqlite as schedules_sqlite
from schlange.services.tasks import background as tasks_background
from schlange.services.tasks import core as tasks_core
from schlange.services.tasks import sqlite as tasks_sqlite

LOGGER = logging.getLogger(__name__)

DEFAULT_TASK_DATABASE_PATH = pathlib.Path("tasks.db")
DEFAULT_SCHEDULE_DATABASE_PATH = pathlib.Path("schedules.db")
DEFAULT_RETRY_POLICY = tasks_core.RetryPolicy(
    initial_delay=1,
    backoff_factor=2.0,
    max_delay=60 * 60 * 24,
    max_attempts=20,
)

DEFAULT_EXECUTOR_INTERVAL = 1
DEFAULT_EXECUTOR_THREADS = os.cpu_count() or 4

DEFAULT_CLEANUP_POLICY = tasks_core.CleanupPolicy(
    delete_succeeded_after=60 * 60 * 24,
    delete_failed_after=60 * 60 * 24 * 7,
)
DEFAULT_CLEANUP_WORKER_INTERVAL = 60

DEFAULT_SCHEDULE_WORKER_INTERVAL = 1


@dataclasses.dataclass
class Schlange:

    task_service: tasks_core.TaskService
    default_retry_policy: tasks_core.RetryPolicy
    schedule_service: schedules_core.ScheduleService
    executor: execution_background.Executor
    cleanup_worker: tasks_background.CleanupWorker
    schedule_worker: schedules_background.ScheduleWorker

    def __enter__(self) -> "Schlange":
        self.start()
        return self

    def __exit__(self, *exc) -> None:
        self.stop()

    def start(self) -> None:
        self.executor.start()
        self.cleanup_worker.start()
        self.schedule_worker.start()

    def stop(self) -> None:
        self.cleanup_worker.stop()
        self.executor.stop()
        self.schedule_worker.stop()

    @classmethod
    @contextlib.contextmanager
    def new(
        cls,
        task_database_path: pathlib.Path = DEFAULT_TASK_DATABASE_PATH,
        schedule_database_path: pathlib.Path = DEFAULT_SCHEDULE_DATABASE_PATH,
        task_handler: Optional[tasks_core.TaskHandler] = None,
        default_retry_policy: tasks_core.RetryPolicy = DEFAULT_RETRY_POLICY,
        executor_interval: float = DEFAULT_EXECUTOR_INTERVAL,
        executor_threads: int = DEFAULT_EXECUTOR_THREADS,
        cleanup_policy: tasks_core.CleanupPolicy = DEFAULT_CLEANUP_POLICY,
        cleanup_worker_interval: float = DEFAULT_CLEANUP_WORKER_INTERVAL,
        schedule_worker_interval: float = DEFAULT_SCHEDULE_WORKER_INTERVAL,
    ) -> Generator["Schlange", None, None]:
        read_pool_capacity = calculate_optimal_database_read_pool_capacity(
            executor_threads
        )
        with sqlite.Database.open(
            path=task_database_path,
            read_pool_capacity=read_pool_capacity,
        ) as task_db, sqlite.Database.open(
            path=schedule_database_path,
            read_pool_capacity=read_pool_capacity,
        ) as schedule_db:
            task_db.migrate(migrations_path=tasks_sqlite.MIGRATIONS_PATH)
            schedule_db.migrate(migrations_path=schedules_sqlite.MIGRATIONS_PATH)
            task_repository = tasks_sqlite.TaskRepository(db=task_db)
            task_service = tasks_core.TaskService(
                task_repository=task_repository,
            )
            schedule_repository = schedules_sqlite.ScheduleRepository(db=schedule_db)
            schedule_service = schedules_core.ScheduleService(
                schedule_repository=schedule_repository,
                task_service=task_service,
            )
            executor = execution_background.Executor(
                interval=executor_interval,
                task_service=task_service,
                threads=executor_threads,
            )
            cleanup_worker = tasks_background.CleanupWorker(
                interval=cleanup_worker_interval,
                task_service=task_service,
                cleanup_policy=cleanup_policy,
            )
            schedule_worker = schedules_background.ScheduleWorker(
                interval=schedule_worker_interval,
                schedule_service=schedule_service,
            )
            yield cls(
                task_service=task_service,
                default_retry_policy=default_retry_policy,
                schedule_service=schedule_service,
                executor=executor,
                cleanup_worker=cleanup_worker,
                schedule_worker=schedule_worker,
            )

    def create_task(
        self,
        args: core.DTO,
        kind: str,
        delay: float = 0.0,
        retry_policy: Optional[tasks_core.RetryPolicy] = None,
        id: Optional[str] = None,
    ) -> tasks_core.Task:
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
            kind=kind,
            delay=delay,
            retry_policy=retry_policy,
            id=id,
        )
        LOGGER.info("task created: task=%r", task)
        return task

    def task(self, task_id: str) -> tasks_core.Task:
        return self.task_service.task(task_id)

    def delete_task(self, task_id: str) -> None:
        self.task_service.delete_task(task_id)

    def tasks(
        self, state: Optional[tasks_core.TaskState] = None
    ) -> List[tasks_core.Task]:
        spec = tasks_core.TaskSpecification(state=state)
        return self.task_service.list_tasks(spec=spec)

    def reactivate_task(self, task_id: str, delay: float = 0) -> tasks_core.Task:
        return self.task_service.reactivate_task(task_id=task_id, delay=delay)

    def create_schedule(
        self,
        task_args: core.DTO,
        task_kind: str,
        interval: float,
        enabled: bool = True,
        delay: float = 0.0,
        retry_policy: Optional[tasks_core.RetryPolicy] = None,
        task_retry_policy: Optional[tasks_core.RetryPolicy] = None,
        id: Optional[str] = None,
    ) -> schedules_core.Schedule:
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
            task_kind=task_kind,
            task_retry_policy=task_retry_policy,
            id=id,
        )
        LOGGER.info("schedule created: schedule=%r", schedule)
        return schedule

    def schedule(self, schedule_id: str) -> schedules_core.Schedule:
        return self.schedule_service.schedule(schedule_id)

    def schedules(
        self, enabled: Optional[bool] = None
    ) -> List[schedules_core.Schedule]:
        return self.schedule_service.list_schedules(
            schedules_core.ScheduleSpecification(enabled=enabled)
        )

    def delete_schedule(self, schedule_id: str) -> None:
        self.schedule_service.delete_schedule(schedule_id)


new = Schlange.new


def calculate_optimal_database_read_pool_capacity(executor_threads: int) -> int:
    executor = 1
    schedule_worker = 1
    cleanup_worker = 1
    additional_capacity = 1
    return (
        executor
        + executor_threads
        + cleanup_worker
        + schedule_worker
        + additional_capacity
    )
