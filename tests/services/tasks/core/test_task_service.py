import pathlib
import tempfile
import unittest
from typing import List

from schlange.internal import core as internal_core
from schlange.internal import sqlite
from schlange.services.tasks import core as tasks_core
from schlange.services.tasks import sqlite as tasks_sqlite


def _retry_policy():
    return internal_core.RetryPolicy(
        initial_delay=1,
        backoff_factor=2,
        max_delay=None,
        max_attempts=3,
    )


def _immediate_retry_policy():
    return internal_core.RetryPolicy(
        initial_delay=0,
        backoff_factor=2,
        max_delay=None,
        max_attempts=3,
    )


class FakeMessageQueue:

    def __init__(self) -> None:
        self.published: List[tasks_core.TaskExecutionRequest] = []

    def publish(self, request: tasks_core.TaskExecutionRequest) -> None:
        self.published.append(request)


class FakeLeaseService:

    def __init__(self, acquired: bool = True) -> None:
        self._acquired = acquired
        self.calls: List[tuple[str, str, float]] = []

    def acquire_lease(self, key: str, holder: str, ttl: float) -> bool:
        self.calls.append((key, holder, ttl))
        return self._acquired


class TaskServiceDispatchTest(unittest.TestCase):

    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        db_path = pathlib.Path(self.dir.name) / "tasks.db"
        self.db_ctx = sqlite.Database.open(db_path, read_pool_capacity=4)
        self.db = self.db_ctx.__enter__()
        self.db.migrate(migrations_path=tasks_sqlite.MIGRATIONS_PATH)
        task_repository = tasks_sqlite.TaskRepository(self.db)
        self.message_queue = FakeMessageQueue()
        self.lease_service = FakeLeaseService()
        self.task_service = tasks_core.TaskService(
            task_repository=task_repository,
            message_queue=self.message_queue,
            lease_service=self.lease_service,
        )

    def tearDown(self):
        self.db_ctx.__exit__(None, None, None)
        self.dir.cleanup()

    def _create_task(self, **overrides):
        defaults = dict(
            args={"key": "value"},
            kind="test_kind",
            delay=0,
            visibility_timeout=30.0,
            retry_policy=_retry_policy(),
        )
        defaults.update(overrides)
        return self.task_service.create_task(**defaults)

    def test_executable_tasks_returns_ready_active_undispatched(self):
        ready = self._create_task(kind="ready", delay=0)
        delayed = self._create_task(kind="delayed", delay=3600)
        result = self.task_service.executable_tasks()
        ids = {t.id for t in result}
        self.assertIn(ready.id, ids)
        self.assertNotIn(delayed.id, ids)

    def test_executable_tasks_excludes_tasks_with_outstanding_execution(self):
        task = self._create_task(kind="ready", delay=0)
        self.task_service.begin_execution(task.id)
        result = self.task_service.executable_tasks()
        ids = {t.id for t in result}
        self.assertNotIn(task.id, ids)

    def test_executable_tasks_reincludes_task_after_execution_ended(self):
        task = self._create_task(
            kind="ready", delay=0, retry_policy=_immediate_retry_policy()
        )
        self.task_service.begin_execution(task.id)
        loaded = self.task_service.task(task.id)
        self.task_service.end_execution(
            task_id=task.id, seq_num=loaded.last_execution.seq_num, error="boom"
        )
        result = self.task_service.executable_tasks()
        ids = {t.id for t in result}
        self.assertIn(task.id, ids)

    def test_begin_execution_begins_publishes_and_saves(self):
        task = self._create_task(kind="test_kind", args={"a": 1})
        self.task_service.begin_execution(task.id)
        loaded = self.task_service.task(task.id)
        self.assertIsNotNone(loaded.last_execution)
        self.assertEqual(loaded.last_execution.seq_num, 0)
        self.assertFalse(loaded.last_execution.ended)
        self.assertEqual(len(self.message_queue.published), 1)

    def test_begin_execution_publishes_correct_payload(self):
        task = self._create_task(kind="test_kind", args={"a": 1})
        self.task_service.begin_execution(task.id)
        request = self.message_queue.published[0]
        self.assertEqual(request.kind, "test_kind")
        self.assertEqual(request.task_id, task.id)
        self.assertEqual(request.seq_num, 0)
        self.assertEqual(request.args, {"a": 1})
        self.assertEqual(request.visibility_timeout, task.visibility_timeout)

    def test_begin_execution_uses_seq_num_per_task(self):
        task = self._create_task(
            kind="test_kind", retry_policy=_immediate_retry_policy()
        )
        self.task_service.begin_execution(task.id)
        loaded = self.task_service.task(task.id)
        self.task_service.end_execution(
            task_id=task.id, seq_num=loaded.last_execution.seq_num, error="boom"
        )
        self.task_service.begin_execution(task.id)
        loaded = self.task_service.task(task.id)
        self.assertEqual(loaded.last_execution.seq_num, 1)
        request = self.message_queue.published[-1]
        self.assertEqual(request.seq_num, 1)

    def test_begin_execution_raises_on_outstanding_execution(self):
        task = self._create_task(kind="test_kind")
        self.task_service.begin_execution(task.id)
        with self.assertRaises(tasks_core.TaskExecutionNotEndedYetError):
            self.task_service.begin_execution(task.id)

    def test_begin_execution_raises_when_not_active(self):
        task = self._create_task(kind="test_kind")
        self.task_service.begin_execution(task.id)
        loaded = self.task_service.task(task.id)
        self.task_service.end_execution(
            task_id=task.id, seq_num=loaded.last_execution.seq_num, error=None
        )
        with self.assertRaises(tasks_core.TaskNotActiveError):
            self.task_service.begin_execution(task.id)


if __name__ == "__main__":
    unittest.main()
