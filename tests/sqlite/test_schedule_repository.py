import datetime
import pathlib
import tempfile
import unittest

from schlange.internal import sqlite
from schlange.services.schedules import core
from schlange.services.schedules import sqlite as schedules_sqlite
from schlange.services.tasks import core as tasks_core


class ScheduleRepositoryTest(unittest.TestCase):

    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        db_path = pathlib.Path(self.dir.name) / "test.db"
        self.db_ctx = sqlite.Database.open(db_path, read_pool_capacity=4)
        self.db = self.db_ctx.__enter__()
        self.db.migrate(migrations_path=schedules_sqlite.MIGRATIONS_PATH)
        self.repo = schedules_sqlite.ScheduleRepository(self.db)

    def tearDown(self):
        self.db_ctx.__exit__(None, None, None)
        self.dir.cleanup()

    def test_dump_load_schedule(self):
        schedule = core.Schedule.create(
            now=datetime.datetime.now(datetime.UTC),
            id="test-id",
            delay=0,
            interval=0.5,
            retry_policy=tasks_core.RetryPolicy(
                initial_delay=1, backoff_factor=2, max_delay=None, max_attempts=3
            ),
            enabled=True,
            task_args={"key": "value"},
            task_retry_policy=tasks_core.RetryPolicy(
                initial_delay=1, backoff_factor=2, max_delay=None, max_attempts=1
            ),
        )
        self.repo.create_schedule(schedule)
        loaded = self.repo.get_schedule("test-id")
        self.assertEqual(loaded, schedule)
