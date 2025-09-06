import json
import sqlite3
from typing import List

from schlange import core

from .data_mapper import DataMapper
from .database import Database
from .errors import NoRowsError

SQL_CREATE_TASK = """
    INSERT INTO tasks (id, version, created_at, args, state, ready_at, retry_policy,
        executions, last_execution_ended_at, schedule_id)
    VALUES (:id, :version, :created_at, :args, :state, :ready_at, :retry_policy,
        :executions, :last_execution_ended_at, :schedule_id)
"""

SQL_GET_TASK_BY_ID = """
    SELECT id, version, created_at, args, state, ready_at, retry_policy, executions,
        schedule_id
    FROM tasks
    WHERE id = :id
"""

SQL_GET_TASKS_BY_SPEC = """
    SELECT id, version, created_at, args, state, ready_at, retry_policy, executions,
        schedule_id
    FROM tasks
    WHERE
        coalesce(state = :state, true) AND
        coalesce(ready_at <= :ready_as_of, true) AND
        coalesce(last_execution_ended_at <= :last_execution_ended_before, true)
"""

SQL_DELETE_TASK_BY_ID = """
    DELETE
    FROM tasks
    WHERE id = :id
"""

SQL_UPDATE_TASK_BY_ID = """
    UPDATE tasks
    SET
        version = :version + 1,
        created_at = :created_at,
        args = :args,
        state = :state,
        ready_at = :ready_at,
        retry_policy = :retry_policy,
        executions = :executions,
        last_execution_ended_at = :last_execution_ended_at,
        schedule_id = :schedule_id
    WHERE id = :id AND version = :version
"""


class TaskRepository:

    def __init__(self, db: Database) -> None:
        self.db = db
        self.data_mapper = DataMapper()

    def create_task(self, task: core.Task) -> None:
        with self.db.transaction() as tx:
            try:
                tx.execute(
                    SQL_CREATE_TASK,
                    {
                        "id": task.id,
                        "version": task.version,
                        "created_at": self.data_mapper.dump_timestamp(task.created_at),
                        "args": json.dumps(task.args),
                        "state": self.data_mapper.dump_task_state(task.state),
                        "ready_at": self.data_mapper.dump_timestamp(task.ready_at),
                        "retry_policy": json.dumps(
                            self.data_mapper.dump_retry_policy(task.retry_policy)
                        ),
                        "executions": json.dumps(
                            [
                                self.data_mapper.dump_task_execution(execution)
                                for execution in task.executions
                            ]
                        ),
                        "last_execution_ended_at": (
                            self.data_mapper.dump_timestamp(
                                task.last_execution.ended_at
                            )
                            if task.last_execution is not None
                            and task.last_execution.ended_at is not None
                            else None
                        ),
                        "schedule_id": task.schedule_id,
                    },
                )
            except sqlite3.IntegrityError:
                raise core.TaskAlreadyExistsError()

    def get_task(self, task_id: str) -> core.Task:
        with self.db.transaction(read_only=True) as tx:
            try:
                row = tx.query_row(SQL_GET_TASK_BY_ID, {"id": task_id})
            except NoRowsError:
                raise core.TaskNotFoundError()
            return self._collect_task(row)

    def list_tasks(self, spec: core.TaskSpecification) -> List[core.Task]:
        with self.db.transaction(read_only=True) as tx:
            rows = tx.query(
                SQL_GET_TASKS_BY_SPEC,
                {
                    "state": spec.state.value if spec.state is not None else None,
                    "ready_as_of": (
                        spec.ready_as_of.isoformat()
                        if spec.ready_as_of is not None
                        else None
                    ),
                    "last_execution_ended_before": (
                        spec.last_execution_ended_before.isoformat()
                        if spec.last_execution_ended_before is not None
                        else None
                    ),
                },
            )
            return [self._collect_task(row) for row in rows]

    def _collect_task(self, row: sqlite3.Row) -> core.Task:
        return core.Task(
            id=row[0],
            version=row[1],
            created_at=self.data_mapper.load_timestamp(row[2]),
            args=json.loads(row[3]),
            state=self.data_mapper.load_task_state(row[4]),
            ready_at=self.data_mapper.load_timestamp(row[5]),
            retry_policy=self.data_mapper.load_retry_policy(json.loads(row[6])),
            executions=[
                self.data_mapper.load_task_execution(dto) for dto in json.loads(row[7])
            ],
            schedule_id=row[8],
        )

    def delete_task(self, task_id: str) -> None:
        with self.db.transaction() as tx:
            rows_affected = tx.execute(SQL_DELETE_TASK_BY_ID, {"id": task_id})
            if not rows_affected:
                raise core.TaskNotFoundError()

    def update_task(self, task: core.Task, synchronous: bool) -> None:
        with self.db.transaction(synchronous=synchronous) as tx:
            rows_affected = tx.execute(
                SQL_UPDATE_TASK_BY_ID,
                {
                    "id": task.id,
                    "version": task.version,
                    "created_at": self.data_mapper.dump_timestamp(task.created_at),
                    "args": json.dumps(task.args),
                    "state": self.data_mapper.dump_task_state(task.state),
                    "ready_at": self.data_mapper.dump_timestamp(task.ready_at),
                    "retry_policy": json.dumps(
                        self.data_mapper.dump_retry_policy(task.retry_policy)
                    ),
                    "executions": json.dumps(
                        [
                            self.data_mapper.dump_task_execution(execution)
                            for execution in task.executions
                        ]
                    ),
                    "last_execution_ended_at": (
                        self.data_mapper.dump_timestamp(task.last_execution.ended_at)
                        if task.last_execution is not None
                        and task.last_execution.ended_at is not None
                        else None
                    ),
                    "schedule_id": task.schedule_id,
                },
            )
            if not rows_affected:
                raise core.TaskUpdatedConcurrentlyError()
