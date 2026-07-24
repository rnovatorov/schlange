import dataclasses
import datetime
import pathlib
import tempfile
import unittest
from typing import List
from unittest import mock

from schlange.api import tasks as tasks_api
from schlange.internal import sqlite
from schlange.services.tasks import api as tasks_api_service
from schlange.services.tasks import core as tasks_core
from schlange.services.tasks import sqlite as tasks_sqlite


def _retry_policy():
    return tasks_api.RetryPolicy(
        initial_delay=1,
        backoff_factor=2,
        max_delay=None,
        max_attempts=3,
    )


class FakeMessageQueue:

    def __init__(self) -> None:
        self.published: List[tasks_core.TaskExecutionRequest] = []

    def publish(self, request: tasks_core.TaskExecutionRequest) -> None:
        self.published.append(request)


class TaskServerTest(unittest.TestCase):

    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        db_path = pathlib.Path(self.dir.name) / "test.db"
        self.db_ctx = sqlite.Database.open(db_path, read_pool_capacity=4)
        self.db = self.db_ctx.__enter__()
        self.db.migrate(migrations_path=tasks_sqlite.MIGRATIONS_PATH)
        task_repository = tasks_sqlite.TaskRepository(self.db)
        self.message_queue = FakeMessageQueue()
        self.task_service = tasks_core.TaskService(
            task_repository=task_repository,
            message_queue=self.message_queue,
            lease_service=mock.MagicMock(),
        )
        self.server = tasks_api_service.Server(service=self.task_service)

    def tearDown(self):
        self.db_ctx.__exit__(None, None, None)
        self.dir.cleanup()

    def _dispatch_and_get_seq_num(self, task_id: str) -> int:
        self.task_service.begin_execution(task_id)
        task = self.task_service.task(task_id)
        assert task.last_execution is not None
        return task.last_execution.seq_num

    def test_create_task_returns_properly_typed_task(self):
        response = self.server.create_task(
            tasks_api.CreateTaskRequest(
                kind="test_kind",
                args={"key": "value"},
                delay=0,
                retry_policy=_retry_policy(),
            )
        )
        task = response.task
        self.assertEqual(task.kind, "test_kind")
        self.assertEqual(task.args, {"key": "value"})
        self.assertEqual(task.state, tasks_api.TaskState.ACTIVE)
        self.assertIsInstance(task.state, tasks_api.TaskState)
        self.assertIsInstance(task.id, str)
        self.assertEqual(
            {f.name for f in dataclasses.fields(task)},
            {"id", "kind", "args", "state"},
        )

    def test_get_task(self):
        created = self.server.create_task(
            tasks_api.CreateTaskRequest(
                kind="test_kind",
                args={"key": "value"},
                delay=0,
                retry_policy=_retry_policy(),
            )
        )
        response = self.server.get_task(tasks_api.GetTaskRequest(id=created.task.id))
        task = response.task
        self.assertEqual(task.id, created.task.id)
        self.assertEqual(task.kind, "test_kind")
        self.assertEqual(task.args, {"key": "value"})
        self.assertEqual(task.state, tasks_api.TaskState.ACTIVE)

    def test_list_tasks_by_state(self):
        self.server.create_task(
            tasks_api.CreateTaskRequest(
                kind="test_kind",
                args={},
                delay=0,
                retry_policy=_retry_policy(),
            )
        )
        response = self.server.list_tasks(
            tasks_api.ListTasksRequest(state=tasks_api.TaskState.ACTIVE)
        )
        self.assertEqual(len(response.tasks), 1)
        self.assertEqual(response.tasks[0].state, tasks_api.TaskState.ACTIVE)

    def test_delete_task(self):
        created = self.server.create_task(
            tasks_api.CreateTaskRequest(
                kind="test_kind",
                args={},
                delay=0,
                retry_policy=_retry_policy(),
            )
        )
        self.server.delete_task(tasks_api.DeleteTaskRequest(id=created.task.id))
        with self.assertRaises(tasks_core.TaskNotFoundError):
            self.server.get_task(tasks_api.GetTaskRequest(id=created.task.id))

    def test_end_execution_success(self):
        created = self.server.create_task(
            tasks_api.CreateTaskRequest(
                kind="test_kind",
                args={},
                delay=0,
                retry_policy=_retry_policy(),
            )
        )
        seq_num = self._dispatch_and_get_seq_num(created.task.id)
        self.server.end_execution(
            tasks_api.EndExecutionRequest(
                task_id=created.task.id,
                seq_num=seq_num,
                error=None,
            )
        )
        task = self.server.get_task(tasks_api.GetTaskRequest(id=created.task.id)).task
        self.assertEqual(task.state, tasks_api.TaskState.SUCCEEDED)

    def test_end_execution_failure_retries(self):
        created = self.server.create_task(
            tasks_api.CreateTaskRequest(
                kind="test_kind",
                args={},
                delay=0,
                retry_policy=_retry_policy(),
            )
        )
        before = self.task_service.task(created.task.id).ready_at
        seq_num = self._dispatch_and_get_seq_num(created.task.id)
        self.server.end_execution(
            tasks_api.EndExecutionRequest(
                task_id=created.task.id,
                seq_num=seq_num,
                error="boom",
            )
        )
        core_task = self.task_service.task(created.task.id)
        self.assertEqual(core_task.state, tasks_core.TaskState.ACTIVE)
        self.assertGreater(core_task.ready_at, before)

    def test_end_execution_failure_max_attempts(self):
        retry_policy = tasks_api.RetryPolicy(
            initial_delay=0,
            backoff_factor=2,
            max_delay=None,
            max_attempts=3,
        )
        created = self.server.create_task(
            tasks_api.CreateTaskRequest(
                kind="test_kind",
                args={},
                delay=0,
                retry_policy=retry_policy,
            )
        )
        for _ in range(3):
            seq_num = self._dispatch_and_get_seq_num(created.task.id)
            self.server.end_execution(
                tasks_api.EndExecutionRequest(
                    task_id=created.task.id,
                    seq_num=seq_num,
                    error="boom",
                )
            )
        task = self.server.get_task(tasks_api.GetTaskRequest(id=created.task.id)).task
        self.assertEqual(task.state, tasks_api.TaskState.FAILED)

    def test_end_execution_duplicate_is_noop(self):
        created = self.server.create_task(
            tasks_api.CreateTaskRequest(
                kind="test_kind",
                args={},
                delay=0,
                retry_policy=_retry_policy(),
            )
        )
        seq_num = self._dispatch_and_get_seq_num(created.task.id)
        self.server.end_execution(
            tasks_api.EndExecutionRequest(
                task_id=created.task.id,
                seq_num=seq_num,
                error=None,
            )
        )
        self.server.end_execution(
            tasks_api.EndExecutionRequest(
                task_id=created.task.id,
                seq_num=seq_num,
                error="late",
            )
        )
        task = self.server.get_task(tasks_api.GetTaskRequest(id=created.task.id)).task
        self.assertEqual(task.state, tasks_api.TaskState.SUCCEEDED)

    def test_end_execution_unknown_seq_num_raises(self):
        created = self.server.create_task(
            tasks_api.CreateTaskRequest(
                kind="test_kind",
                args={},
                delay=0,
                retry_policy=_retry_policy(),
            )
        )
        with self.assertRaises(tasks_core.TaskExecutionNotFoundError):
            self.server.end_execution(
                tasks_api.EndExecutionRequest(
                    task_id=created.task.id,
                    seq_num=999,
                    error=None,
                )
            )

    def test_list_tasks_with_spec_filters(self):
        delayed = self.server.create_task(
            tasks_api.CreateTaskRequest(
                kind="delayed",
                args={},
                delay=3600,
                retry_policy=_retry_policy(),
            )
        )
        ready = self.server.create_task(
            tasks_api.CreateTaskRequest(
                kind="ready",
                args={},
                delay=0,
                retry_policy=_retry_policy(),
            )
        )
        now = datetime.datetime.now(datetime.UTC)
        past = now - datetime.timedelta(seconds=3600)

        completed = self.server.create_task(
            tasks_api.CreateTaskRequest(
                kind="completed",
                args={},
                delay=0,
                retry_policy=_retry_policy(),
            )
        )
        seq_num = self._dispatch_and_get_seq_num(completed.task.id)
        self.server.end_execution(
            tasks_api.EndExecutionRequest(
                task_id=completed.task.id,
                seq_num=seq_num,
                error=None,
            )
        )
        ended_at = datetime.datetime.now(datetime.UTC)

        response = self.server.list_tasks(
            tasks_api.ListTasksRequest(
                state=tasks_api.TaskState.ACTIVE, ready_as_of=now
            )
        )
        ids = {t.id for t in response.tasks}
        self.assertIn(ready.task.id, ids)
        self.assertNotIn(delayed.task.id, ids)

        response = self.server.list_tasks(
            tasks_api.ListTasksRequest(
                state=tasks_api.TaskState.ACTIVE,
                ready_as_of=now + datetime.timedelta(seconds=3600),
            )
        )
        ids = {t.id for t in response.tasks}
        self.assertIn(delayed.task.id, ids)

        response = self.server.list_tasks(
            tasks_api.ListTasksRequest(
                state=tasks_api.TaskState.SUCCEEDED, last_execution_ended_before=past
            )
        )
        ids = {t.id for t in response.tasks}
        self.assertNotIn(completed.task.id, ids)

        response = self.server.list_tasks(
            tasks_api.ListTasksRequest(
                state=tasks_api.TaskState.SUCCEEDED,
                last_execution_ended_before=ended_at + datetime.timedelta(seconds=3600),
            )
        )
        ids = {t.id for t in response.tasks}
        self.assertIn(completed.task.id, ids)

    def test_begin_execution_on_succeeded_task_raises_not_active(self):
        created = self.server.create_task(
            tasks_api.CreateTaskRequest(
                kind="test_kind",
                args={},
                delay=0,
                retry_policy=_retry_policy(),
            )
        )
        seq_num = self._dispatch_and_get_seq_num(created.task.id)
        self.server.end_execution(
            tasks_api.EndExecutionRequest(
                task_id=created.task.id,
                seq_num=seq_num,
                error=None,
            )
        )
        with self.assertRaises(tasks_core.TaskNotActiveError):
            self.task_service.begin_execution(created.task.id)

    def test_begin_execution_on_non_ready_task_raises(self):
        created = self.server.create_task(
            tasks_api.CreateTaskRequest(
                kind="test_kind",
                args={},
                delay=3600,
                retry_policy=_retry_policy(),
            )
        )
        with self.assertRaises(tasks_core.TaskNotReadyError):
            self.task_service.begin_execution(created.task.id)

    def test_unknown_task_id_raises_not_found(self):
        unknown_id = "nonexistent-task-id"
        with self.assertRaises(tasks_core.TaskNotFoundError):
            self.server.get_task(tasks_api.GetTaskRequest(id=unknown_id))
        with self.assertRaises(tasks_core.TaskNotFoundError):
            self.server.delete_task(tasks_api.DeleteTaskRequest(id=unknown_id))
        with self.assertRaises(tasks_core.TaskNotFoundError):
            self.server.end_execution(
                tasks_api.EndExecutionRequest(
                    task_id=unknown_id,
                    seq_num=0,
                    error=None,
                )
            )

    def test_reactivate_task(self):
        retry_policy = tasks_api.RetryPolicy(
            initial_delay=0,
            backoff_factor=2,
            max_delay=None,
            max_attempts=3,
        )
        created = self.server.create_task(
            tasks_api.CreateTaskRequest(
                kind="test_kind",
                args={},
                delay=0,
                retry_policy=retry_policy,
            )
        )
        for _ in range(3):
            seq_num = self._dispatch_and_get_seq_num(created.task.id)
            self.server.end_execution(
                tasks_api.EndExecutionRequest(
                    task_id=created.task.id,
                    seq_num=seq_num,
                    error="boom",
                )
            )
        self.assertEqual(
            self.server.get_task(
                tasks_api.GetTaskRequest(id=created.task.id)
            ).task.state,
            tasks_api.TaskState.FAILED,
        )
        response = self.server.reactivate_task(
            tasks_api.ReactivateTaskRequest(id=created.task.id, delay=0)
        )
        self.assertEqual(response.task.state, tasks_api.TaskState.ACTIVE)


class DataMapperTest(unittest.TestCase):

    def setUp(self):
        self.data_mapper = tasks_api_service.DataMapper()

    def test_dump_task_produces_typed_state(self):
        core_task = tasks_core.Task(
            id="task-1",
            version=1,
            created_at=datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC),
            state=tasks_core.TaskState.ACTIVE,
            kind="test_kind",
            args={"key": "value"},
            ready_at=datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC),
            retry_policy=tasks_core.RetryPolicy(
                initial_delay=1,
                backoff_factor=2,
                max_delay=None,
                max_attempts=3,
            ),
            executions=[],
            schedule_id=None,
        )
        task = self.data_mapper.dump_task(core_task)
        self.assertIsInstance(task, tasks_api.Task)
        self.assertEqual(task.id, "task-1")
        self.assertEqual(task.kind, "test_kind")
        self.assertEqual(task.args, {"key": "value"})
        self.assertIsInstance(task.state, tasks_api.TaskState)
        self.assertEqual(task.state, tasks_api.TaskState.ACTIVE)

    def test_dump_and_load_retry_policy_roundtrip(self):
        core_policy = tasks_core.RetryPolicy(
            initial_delay=1,
            backoff_factor=2,
            max_delay=60,
            max_attempts=5,
        )
        api_policy = self.data_mapper.dump_retry_policy(core_policy)
        self.assertIsInstance(api_policy, tasks_api.RetryPolicy)
        self.assertEqual(api_policy.initial_delay, 1)
        self.assertEqual(api_policy.backoff_factor, 2)
        self.assertEqual(api_policy.max_delay, 60)
        self.assertEqual(api_policy.max_attempts, 5)
        roundtripped = self.data_mapper.load_retry_policy(api_policy)
        self.assertIsInstance(roundtripped, tasks_core.RetryPolicy)
        self.assertEqual(roundtripped, core_policy)


if __name__ == "__main__":
    unittest.main()
