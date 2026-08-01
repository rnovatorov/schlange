import pathlib
import tempfile
import unittest

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


def _now():
    import datetime

    return datetime.datetime.now(datetime.UTC)


class TaskRepositoryExecutionInProgressTest(unittest.TestCase):

    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        db_path = pathlib.Path(self.dir.name) / "tasks.db"
        self.db_ctx = sqlite.Database.open(db_path, read_pool_capacity=4)
        self.db = self.db_ctx.__enter__()
        self.db.migrate(migrations_path=tasks_sqlite.MIGRATIONS_PATH)
        self.repository = tasks_sqlite.TaskRepository(self.db)

    def tearDown(self):
        self.db_ctx.__exit__(None, None, None)
        self.dir.cleanup()

    def _create_task(self, task_id, delay=0):
        task = tasks_core.Task.create(
            now=_now(),
            id=task_id,
            kind="test_kind",
            args={},
            delay=delay,
            retry_policy=_retry_policy(),
            visibility_timeout=30.0,
            schedule_id=None,
        )
        self.repository.create_task(task)
        return task

    def _begin_and_save(self, task_id):
        task = self.repository.get_task(task_id)
        task.begin_execution(now=_now())
        self.repository.update_task(task, synchronous=False)
        return task

    def _end_and_save(self, task_id, seq_num, error="boom"):
        task = self.repository.get_task(task_id)
        task.end_execution(seq_num=seq_num, now=_now(), error=error)
        self.repository.update_task(task, synchronous=True)
        return task

    def test_execution_in_progress_true_returns_only_outstanding(self):
        none = self._create_task("none")
        begun = self._begin_and_save(self._create_task("begun").id)
        ended = self._create_task("ended")
        ended = self._begin_and_save(ended.id)
        self._end_and_save(ended.id, ended.last_execution.seq_num)

        result = self.repository.list_tasks(
            tasks_core.TaskSpecification(execution_in_progress=True)
        )
        ids = {t.id for t in result}
        self.assertIn(begun.id, ids)
        self.assertNotIn(none.id, ids)
        self.assertNotIn(ended.id, ids)

    def test_execution_in_progress_false_returns_none_and_ended(self):
        none = self._create_task("none")
        begun = self._begin_and_save(self._create_task("begun").id)
        ended = self._create_task("ended")
        ended = self._begin_and_save(ended.id)
        self._end_and_save(ended.id, ended.last_execution.seq_num)

        result = self.repository.list_tasks(
            tasks_core.TaskSpecification(execution_in_progress=False)
        )
        ids = {t.id for t in result}
        self.assertIn(none.id, ids)
        self.assertIn(ended.id, ids)
        self.assertNotIn(begun.id, ids)

    def test_execution_in_progress_none_returns_all(self):
        none = self._create_task("none")
        begun = self._begin_and_save(self._create_task("begun").id)
        ended = self._create_task("ended")
        ended = self._begin_and_save(ended.id)
        self._end_and_save(ended.id, ended.last_execution.seq_num)

        result = self.repository.list_tasks(
            tasks_core.TaskSpecification(execution_in_progress=None)
        )
        ids = {t.id for t in result}
        self.assertEqual(ids, {none.id, begun.id, ended.id})

    def test_execution_in_progress_combines_with_state_filter(self):
        active = self._create_task("active")
        begun = self._begin_and_save(self._create_task("begun").id)
        ended = self._create_task("ended")
        ended = self._begin_and_save(ended.id)
        self._end_and_save(ended.id, ended.last_execution.seq_num, error=None)

        result = self.repository.list_tasks(
            tasks_core.TaskSpecification(
                state=tasks_core.TaskState.ACTIVE,
                execution_in_progress=False,
            )
        )
        ids = {t.id for t in result}
        self.assertIn(active.id, ids)
        self.assertNotIn(begun.id, ids)
        self.assertNotIn(ended.id, ids)


if __name__ == "__main__":
    unittest.main()
