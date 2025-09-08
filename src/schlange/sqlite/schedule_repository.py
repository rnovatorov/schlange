import json
import sqlite3
from typing import List

from schlange import core

from .data_mapper import DataMapper
from .database import Database
from .errors import NoRowsError

SQL_CREATE_SCHEDULE = """
    INSERT
    INTO schedules (id, version, created_at, ready_at, origin, interval, retry_policy,
        enabled, task_args, task_retry_policy, task_sequence_number, firings)
    VALUES (:id, :version, :created_at, :ready_at, :origin, :interval, :retry_policy,
        :enabled, :task_args, :task_retry_policy, :task_sequence_number, :firings)
"""

SQL_GET_SCHEDULE_BY_ID = """
    SELECT id, version, created_at, ready_at, origin, interval, retry_policy,
        enabled, task_args, task_retry_policy, task_sequence_number, firings
    FROM schedules
    WHERE id = :id
"""

SQL_GET_SCHEDULES_BY_SPEC = """
    SELECT id, version, created_at, ready_at, origin, interval, retry_policy,
        enabled, task_args, task_retry_policy, task_sequence_number, firings
    FROM schedules
    WHERE
        coalesce(enabled = :enabled, true) AND
        coalesce(ready_at <= :ready_as_of, true)
"""

SQL_DELETE_SCHEDULE_BY_ID = """
    DELETE
    FROM schedules
    WHERE id = :id
"""

SQL_UPDATE_SCHEDULE_BY_ID = """
    UPDATE schedules
    SET
        version = version + 1,
        created_at = :created_at,
        ready_at = :ready_at,
        origin = :origin,
        interval = :interval,
        retry_policy = :retry_policy,
        enabled = :enabled,
        task_args = :task_args,
        task_retry_policy = :task_retry_policy,
        task_sequence_number = :task_sequence_number,
        firings = :firings
    WHERE id = :id AND version = :version
"""


class ScheduleRepository:

    def __init__(self, db: Database) -> None:
        self.db = db
        self.data_mapper = DataMapper()

    def create_schedule(self, schedule: core.Schedule) -> None:
        with self.db.transaction() as tx:
            try:
                tx.execute(
                    SQL_CREATE_SCHEDULE,
                    {
                        "id": schedule.id,
                        "version": schedule.version,
                        "created_at": self.data_mapper.dump_timestamp(
                            schedule.created_at
                        ),
                        "ready_at": self.data_mapper.dump_timestamp(schedule.ready_at),
                        "origin": self.data_mapper.dump_timestamp(schedule.origin),
                        "interval": schedule.interval,
                        "retry_policy": json.dumps(
                            self.data_mapper.dump_retry_policy(schedule.retry_policy)
                        ),
                        "enabled": int(schedule.enabled),
                        "task_args": json.dumps(schedule.task_args),
                        "task_retry_policy": json.dumps(
                            self.data_mapper.dump_retry_policy(
                                schedule.task_retry_policy
                            )
                        ),
                        "task_sequence_number": schedule.task_sequence_number,
                        "firings": json.dumps(
                            [
                                self.data_mapper.dump_schedule_firing(firing)
                                for firing in schedule.firings
                            ]
                        ),
                    },
                )
            except sqlite3.IntegrityError:
                raise core.ScheduleAlreadyExistsError()

    def get_schedule(self, schedule_id: str) -> core.Schedule:
        with self.db.transaction(read_only=True) as tx:
            try:
                row = tx.query_row(SQL_GET_SCHEDULE_BY_ID, {"id": schedule_id})
            except NoRowsError:
                raise core.ScheduleNotFoundError() from None
            return self._collect_schedule(row)

    def list_schedules(self, spec: core.ScheduleSpecification) -> List[core.Schedule]:
        with self.db.transaction(read_only=True) as tx:
            rows = tx.query(
                SQL_GET_SCHEDULES_BY_SPEC,
                {
                    "enabled": int(spec.enabled) if spec.enabled is not None else None,
                    "ready_as_of": (
                        spec.ready_as_of.isoformat()
                        if spec.ready_as_of is not None
                        else None
                    ),
                },
            )
            return [self._collect_schedule(row) for row in rows]

    def _collect_schedule(self, row: sqlite3.Row) -> core.Schedule:
        return core.Schedule(
            id=row[0],
            version=row[1],
            created_at=self.data_mapper.load_timestamp(row[2]),
            ready_at=self.data_mapper.load_timestamp(row[3]),
            origin=self.data_mapper.load_timestamp(row[4]),
            interval=row[5],
            retry_policy=self.data_mapper.load_retry_policy(json.loads(row[6])),
            enabled=bool(row[7]),
            task_args=json.loads(row[8]),
            task_retry_policy=self.data_mapper.load_retry_policy(json.loads(row[9])),
            task_sequence_number=row[10],
            firings=[
                self.data_mapper.load_schedule_firing(dto)
                for dto in json.loads(row[11])
            ],
        )

    def delete_schedule(self, schedule_id: str) -> None:
        with self.db.transaction() as tx:
            rows_affected = tx.execute(SQL_DELETE_SCHEDULE_BY_ID, {"id": schedule_id})
            if not rows_affected:
                raise core.ScheduleNotFoundError()

    def update_schedule(self, schedule: core.Schedule, synchronous: bool) -> None:
        with self.db.transaction(synchronous=synchronous) as tx:
            rows_affected = tx.execute(
                SQL_UPDATE_SCHEDULE_BY_ID,
                {
                    "id": schedule.id,
                    "version": schedule.version,
                    "created_at": self.data_mapper.dump_timestamp(schedule.created_at),
                    "ready_at": self.data_mapper.dump_timestamp(schedule.ready_at),
                    "origin": self.data_mapper.dump_timestamp(schedule.origin),
                    "interval": schedule.interval,
                    "retry_policy": json.dumps(
                        self.data_mapper.dump_retry_policy(schedule.retry_policy)
                    ),
                    "enabled": int(schedule.enabled),
                    "task_args": json.dumps(schedule.task_args),
                    "task_retry_policy": json.dumps(
                        self.data_mapper.dump_retry_policy(schedule.task_retry_policy)
                    ),
                    "task_sequence_number": schedule.task_sequence_number,
                    "firings": json.dumps(
                        [
                            self.data_mapper.dump_schedule_firing(firing)
                            for firing in schedule.firings
                        ]
                    ),
                },
            )
            if not rows_affected:
                raise core.ScheduleUpdatedConcurrentlyError()
