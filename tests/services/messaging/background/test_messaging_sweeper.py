import datetime
import pathlib
import tempfile
import unittest
import uuid

from schlange.internal import sqlite
from schlange.services.messaging import core
from schlange.services.messaging import sqlite as messaging_sqlite
from schlange.services.messaging.background import Sweeper


class SweeperTest(unittest.TestCase):

    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        db_path = pathlib.Path(self.dir.name) / "messaging.db"
        self.db_ctx = sqlite.Database.open(db_path, read_pool_capacity=4)
        self.db = self.db_ctx.__enter__()
        self.db.migrate(migrations_path=messaging_sqlite.MIGRATIONS_PATH)
        self.store = messaging_sqlite.Store(self.db)

    def tearDown(self):
        self.db_ctx.__exit__(None, None, None)
        self.dir.cleanup()

    def _now(self) -> datetime.datetime:
        return datetime.datetime.now(datetime.UTC)

    def test_sweep_cleans_up_stale_and_preserves_active(self):
        now = self._now()
        service = core.Service(store=self.store, session_timeout=5.0)
        sweeper = Sweeper(service=service, interval=1.0)

        # Stale session (heartbeat 10s ago) with a claimed message.
        stale_at = now - datetime.timedelta(seconds=10)
        stale_session_id = str(uuid.uuid4())
        stale_message_id = str(uuid.uuid4())
        self.store.create_session(stale_session_id, "orders", False, stale_at)
        self.store.publish(stale_message_id, "orders", b"stale", stale_at)
        self.store.claim(stale_session_id, stale_at)

        # Active session (heartbeat now) with a claimed message.
        fresh_session_id = str(uuid.uuid4())
        fresh_message_id = str(uuid.uuid4())
        self.store.create_session(fresh_session_id, "orders", False, now)
        self.store.publish(fresh_message_id, "orders", b"fresh", now)
        self.store.claim(fresh_session_id, now)

        sweeper.work()

        # Stale session cleaned up: session deleted, claim released.
        self.assertIsNone(self.store.find_session(stale_session_id))
        stale_message = self.store.find_message(stale_message_id)
        self.assertIsNotNone(stale_message)
        self.assertIsNone(stale_message.claimed_by)

        # Active session preserved: session exists, claim intact.
        self.assertIsNotNone(self.store.find_session(fresh_session_id))
        fresh_message = self.store.find_message(fresh_message_id)
        self.assertIsNotNone(fresh_message)
        self.assertEqual(fresh_message.claimed_by, fresh_session_id)


if __name__ == "__main__":
    unittest.main()
