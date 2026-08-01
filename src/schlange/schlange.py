import contextlib
import dataclasses
import logging
import pathlib
import uuid
from typing import Generator, List, Optional

from schlange.internal import core, sqlite
from schlange.services.execution import api as execution_api
from schlange.services.execution import background as execution_background
from schlange.services.execution import core as execution_core
from schlange.services.leases import api as leases_api
from schlange.services.leases import background as leases_background
from schlange.services.leases import core as leases_core
from schlange.services.leases import sqlite as leases_sqlite
from schlange.services.messaging import api as messaging_api
from schlange.services.messaging import core as messaging_core
from schlange.services.messaging import sqlite as messaging_sqlite
from schlange.services.schedules import background as schedules_background
from schlange.services.schedules import core as schedules_core
from schlange.services.schedules import sqlite as schedules_sqlite
from schlange.services.tasks import api as tasks_api
from schlange.services.tasks import background as tasks_background
from schlange.services.tasks import core as tasks_core
from schlange.services.tasks import sqlite as tasks_sqlite

LOGGER = logging.getLogger(__name__)

DEFAULT_TASK_DATABASE_PATH = pathlib.Path("tasks.db")
DEFAULT_SCHEDULE_DATABASE_PATH = pathlib.Path("schedules.db")
DEFAULT_LEASE_DATABASE_PATH = pathlib.Path("leases.db")
DEFAULT_MESSAGING_DATABASE_PATH = pathlib.Path("messaging.db")
DEFAULT_RETRY_POLICY = tasks_core.RetryPolicy(
    initial_delay=1,
    backoff_factor=2.0,
    max_delay=60 * 60 * 24,
    max_attempts=20,
)

DEFAULT_VISIBILITY_TIMEOUT = 30.0
DEFAULT_MAX_DELIVERY_COUNT = 5
DEFAULT_CONSUMER_INTERVAL = 1
DEFAULT_CONSUMERS_PER_KIND = 1

DEFAULT_CLEANUP_POLICY = tasks_core.CleanupPolicy(
    delete_succeeded_after=60 * 60 * 24,
    delete_failed_after=60 * 60 * 24 * 7,
)
DEFAULT_CLEANUP_WORKER_INTERVAL = 60

DEFAULT_SCHEDULE_WORKER_INTERVAL = 1

DEFAULT_DISPATCHER_INTERVAL = 1
DEFAULT_DISPATCHER_LEASE_TTL = 5.0

DEFAULT_LEASE_REAPER_INTERVAL = 60


@dataclasses.dataclass
class Schlange:

    task_service: tasks_core.TaskService
    default_retry_policy: tasks_core.RetryPolicy
    default_visibility_timeout: float
    schedule_service: schedules_core.ScheduleService
    consumers: List[execution_background.Consumer]
    dispatcher: tasks_background.Dispatcher
    cleanup_worker: tasks_background.CleanupWorker
    schedule_worker: schedules_background.ScheduleWorker
    leases_reaper: leases_background.Reaper

    def __enter__(self) -> "Schlange":
        self.start()
        return self

    def __exit__(self, *exc) -> None:
        self.stop()

    def start(self) -> None:
        for consumer in self.consumers:
            consumer.start()
        self.dispatcher.start()
        self.cleanup_worker.start()
        self.schedule_worker.start()
        self.leases_reaper.start()

    def stop(self) -> None:
        workers: list = [
            *self.consumers,
            self.cleanup_worker,
            self.dispatcher,
            self.schedule_worker,
            self.leases_reaper,
        ]
        for w in workers:
            w.cancel()
        errors = []
        for w in workers:
            try:
                w.wait()
            except Exception as e:
                errors.append(e)
        if len(errors) > 0:
            raise ExceptionGroup("Schlange.stop", errors)

    @classmethod
    @contextlib.contextmanager
    def new(
        cls,
        handlers: dict[str, execution_core.Handler] = {},
        task_database_path: pathlib.Path = DEFAULT_TASK_DATABASE_PATH,
        schedule_database_path: pathlib.Path = DEFAULT_SCHEDULE_DATABASE_PATH,
        lease_database_path: pathlib.Path = DEFAULT_LEASE_DATABASE_PATH,
        messaging_database_path: pathlib.Path = DEFAULT_MESSAGING_DATABASE_PATH,
        default_retry_policy: tasks_core.RetryPolicy = DEFAULT_RETRY_POLICY,
        default_visibility_timeout: float = DEFAULT_VISIBILITY_TIMEOUT,
        max_delivery_count: int = DEFAULT_MAX_DELIVERY_COUNT,
        consumer_interval: float = DEFAULT_CONSUMER_INTERVAL,
        consumers_per_kind: int = DEFAULT_CONSUMERS_PER_KIND,
        cleanup_policy: tasks_core.CleanupPolicy = DEFAULT_CLEANUP_POLICY,
        cleanup_worker_interval: float = DEFAULT_CLEANUP_WORKER_INTERVAL,
        schedule_worker_interval: float = DEFAULT_SCHEDULE_WORKER_INTERVAL,
        dispatcher_interval: float = DEFAULT_DISPATCHER_INTERVAL,
        dispatcher_lease_ttl: float = DEFAULT_DISPATCHER_LEASE_TTL,
        lease_reaper_interval: float = DEFAULT_LEASE_REAPER_INTERVAL,
    ) -> Generator["Schlange", None, None]:
        write_pool_capacity = consumers_per_kind * len(handlers)
        read_pool_capacity = calculate_optimal_database_read_pool_capacity(
            consumers_per_kind, len(handlers)
        )
        with sqlite.Database.open(
            path=task_database_path,
            read_pool_capacity=read_pool_capacity,
            write_pool_capacity=write_pool_capacity,
            sync_write_pool_capacity=write_pool_capacity,
        ) as task_db, sqlite.Database.open(
            path=schedule_database_path,
            read_pool_capacity=read_pool_capacity,
        ) as schedule_db, sqlite.Database.open(
            path=lease_database_path,
            read_pool_capacity=read_pool_capacity,
        ) as lease_db, sqlite.Database.open(
            path=messaging_database_path,
            read_pool_capacity=read_pool_capacity,
            write_pool_capacity=write_pool_capacity,
            sync_write_pool_capacity=write_pool_capacity,
        ) as messaging_db:
            task_db.migrate(migrations_path=tasks_sqlite.MIGRATIONS_PATH)
            schedule_db.migrate(migrations_path=schedules_sqlite.MIGRATIONS_PATH)
            lease_db.migrate(migrations_path=leases_sqlite.MIGRATIONS_PATH)
            messaging_db.migrate(migrations_path=messaging_sqlite.MIGRATIONS_PATH)
            task_repository = tasks_sqlite.TaskRepository(db=task_db)
            lease_store = leases_sqlite.Store(db=lease_db)
            messaging_store = messaging_sqlite.Store(db=messaging_db)
            lease_service = leases_core.Service(store=lease_store)
            messaging_service = messaging_core.Service(
                store=messaging_store,
            )
            lease_server = leases_api.Server(service=lease_service)
            messaging_server = messaging_api.Server(service=messaging_service)
            message_queue = tasks_api.MessageQueue(
                messaging_server=messaging_server,
                max_delivery_count=max_delivery_count,
            )
            task_lease_service = tasks_api.LeaseService(lease_server=lease_server)
            task_service = tasks_core.TaskService(
                task_repository=task_repository,
                message_queue=message_queue,
                lease_service=task_lease_service,
            )
            schedule_repository = schedules_sqlite.ScheduleRepository(db=schedule_db)
            schedule_service = schedules_core.ScheduleService(
                schedule_repository=schedule_repository,
                task_service=task_service,
                task_visibility_timeout=default_visibility_timeout,
            )
            task_server = tasks_api.Server(service=task_service)
            task_service_adapter = execution_api.TaskServiceAdapter(
                task_server=task_server,
            )
            execution_service = execution_core.ExecutionService(
                handlers=handlers,
                task_service=task_service_adapter,
            )
            consumers = []
            for kind in handlers:
                for _ in range(consumers_per_kind):
                    consumer = execution_background.Consumer(
                        queue=kind,
                        interval=consumer_interval,
                        messaging_server=messaging_server,
                        execution_service=execution_service,
                    )
                    consumers.append(consumer)
            dispatcher = tasks_background.Dispatcher(
                service=task_service,
                holder=str(uuid.uuid4()),
                key="tasks-dispatcher",
                ttl=dispatcher_lease_ttl,
                interval=dispatcher_interval,
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
            leases_reaper = leases_background.Reaper(
                service=lease_service,
                interval=lease_reaper_interval,
            )
            yield cls(
                task_service=task_service,
                default_retry_policy=default_retry_policy,
                default_visibility_timeout=default_visibility_timeout,
                schedule_service=schedule_service,
                consumers=consumers,
                dispatcher=dispatcher,
                cleanup_worker=cleanup_worker,
                schedule_worker=schedule_worker,
                leases_reaper=leases_reaper,
            )

    def create_task(
        self,
        args: core.DTO,
        kind: str,
        delay: float = 0.0,
        visibility_timeout: Optional[float] = None,
        retry_policy: Optional[tasks_core.RetryPolicy] = None,
        id: Optional[str] = None,
    ) -> tasks_core.Task:
        if retry_policy is None:
            retry_policy = self.default_retry_policy
        if visibility_timeout is None:
            visibility_timeout = self.default_visibility_timeout
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
            visibility_timeout=visibility_timeout,
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


def calculate_optimal_database_read_pool_capacity(
    consumers_per_kind: int, num_kinds: int
) -> int:
    consumers = consumers_per_kind * num_kinds
    background_workers = 4
    additional_capacity = 2
    return consumers + background_workers + additional_capacity
