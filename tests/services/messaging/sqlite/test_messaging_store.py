import datetime
import pathlib
import tempfile
import unittest
import uuid

from schlange.internal import sqlite
from schlange.services.messaging import sqlite as messaging_sqlite


class StoreTest(unittest.TestCase):

    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        db_path = pathlib.Path(self.dir.name) / "messaging.db"
        self.db_ctx = sqlite.Database.open(db_path, read_pool_capacity=4)
        self.db = self.db_ctx.__enter__()
        self.db.migrate(migrations_path=messaging_sqlite.MIGRATIONS_PATH)
        self.store = messaging_sqlite.MessagingStore(self.db)

    def tearDown(self):
        self.db_ctx.__exit__(None, None, None)
        self.dir.cleanup()

    def _now(self) -> datetime.datetime:
        return datetime.datetime.now(datetime.UTC)

    def test_publish(self):
        now = self._now()
        message_id = str(uuid.uuid4())
        self.store.publish(message_id, "orders", b"hello", now)
        message = self.store.find_message(message_id)
        self.assertIsNotNone(message)
        self.assertEqual(message.id, message_id)
        self.assertEqual(message.routing_key, "orders")
        self.assertEqual(message.payload, b"hello")
        self.assertFalse(message.is_dead_letter)
        self.assertIsNone(message.claimed_by)
        self.assertIsNone(message.claimed_at)

    def test_claim_available(self):
        now = self._now()
        message_id = str(uuid.uuid4())
        self.store.publish(message_id, "orders", b"hello", now)
        session_id = str(uuid.uuid4())
        self.store.create_session(session_id, "orders", False, now)
        message = self.store.claim(session_id, now)
        self.assertIsNotNone(message)
        self.assertEqual(message.id, message_id)
        self.assertEqual(message.routing_key, "orders")
        self.assertEqual(message.payload, b"hello")
        found = self.store.find_message(message_id)
        self.assertEqual(found.claimed_by, session_id)

    def test_claim_no_messages(self):
        now = self._now()
        session_id = str(uuid.uuid4())
        self.store.create_session(session_id, "orders", False, now)
        message = self.store.claim(session_id, now)
        self.assertIsNone(message)

    def test_claim_competing_sessions(self):
        now = self._now()
        message_id = str(uuid.uuid4())
        self.store.publish(message_id, "orders", b"hello", now)
        session_a = str(uuid.uuid4())
        self.store.create_session(session_a, "orders", False, now)
        session_b = str(uuid.uuid4())
        self.store.create_session(session_b, "orders", False, now)
        claimed_a = self.store.claim(session_a, now)
        claimed_b = self.store.claim(session_b, now)
        self.assertIsNotNone(claimed_a)
        self.assertIsNone(claimed_b)
        self.assertEqual(claimed_a.id, message_id)

    def test_claim_fifo(self):
        first = self._now()
        second = first + datetime.timedelta(seconds=1)
        id_first = str(uuid.uuid4())
        self.store.publish(id_first, "orders", b"one", first)
        id_second = str(uuid.uuid4())
        self.store.publish(id_second, "orders", b"two", second)
        session_id = str(uuid.uuid4())
        self.store.create_session(session_id, "orders", False, first)
        claimed_first = self.store.claim(session_id, second)
        claimed_second = self.store.claim(session_id, second)
        self.assertIsNotNone(claimed_first)
        self.assertEqual(claimed_first.id, id_first)
        self.assertIsNotNone(claimed_second)
        self.assertEqual(claimed_second.id, id_second)

    def test_dead_letter_claim_isolates_by_flag(self):
        now = self._now()
        message_id = str(uuid.uuid4())
        self.store.publish(message_id, "orders", b"hello", now)
        normal = str(uuid.uuid4())
        self.store.create_session(normal, "orders", False, now)
        self.store.claim(normal, now)
        self.store.nack(message_id)
        self.assertIsNone(self.store.claim(normal, now))
        dead_letter = str(uuid.uuid4())
        self.store.create_session(dead_letter, "orders", True, now)
        claimed = self.store.claim(dead_letter, now)
        self.assertIsNotNone(claimed)
        self.assertEqual(claimed.id, message_id)

    def test_ack_deletes_message(self):
        now = self._now()
        message_id = str(uuid.uuid4())
        self.store.publish(message_id, "orders", b"hello", now)
        session_id = str(uuid.uuid4())
        self.store.create_session(session_id, "orders", False, now)
        self.store.claim(session_id, now)
        self.store.ack(message_id)
        self.assertIsNone(self.store.find_message(message_id))

    def test_nack_moves_to_dead_letter(self):
        now = self._now()
        message_id = str(uuid.uuid4())
        self.store.publish(message_id, "orders", b"hello", now)
        session_id = str(uuid.uuid4())
        self.store.create_session(session_id, "orders", False, now)
        self.store.claim(session_id, now)
        self.store.nack(message_id)
        message = self.store.find_message(message_id)
        self.assertIsNotNone(message)
        self.assertEqual(message.routing_key, "orders")
        self.assertTrue(message.is_dead_letter)
        self.assertIsNone(message.claimed_by)
        self.assertIsNone(message.claimed_at)

    def test_nack_dead_letter_idempotent(self):
        now = self._now()
        message_id = str(uuid.uuid4())
        self.store.publish(message_id, "orders", b"hello", now)
        self.store.nack(message_id)
        self.store.nack(message_id)
        message = self.store.find_message(message_id)
        self.assertIsNotNone(message)
        self.assertTrue(message.is_dead_letter)

    def test_create_session(self):
        now = self._now()
        session_id = str(uuid.uuid4())
        self.store.create_session(session_id, "orders", False, now)
        session = self.store.find_session(session_id)
        self.assertIsNotNone(session)
        self.assertEqual(session.id, session_id)
        self.assertEqual(session.queue, "orders")
        self.assertFalse(session.dead_letter)

    def test_create_session_dead_letter(self):
        now = self._now()
        session_id = str(uuid.uuid4())
        self.store.create_session(session_id, "orders", True, now)
        session = self.store.find_session(session_id)
        self.assertIsNotNone(session)
        self.assertEqual(session.queue, "orders")
        self.assertTrue(session.dead_letter)

    def test_heartbeat_updates_timestamp(self):
        created = self._now()
        session_id = str(uuid.uuid4())
        self.store.create_session(session_id, "orders", False, created)
        later = created + datetime.timedelta(seconds=10)
        self.store.heartbeat(session_id, later)
        session = self.store.find_session(session_id)
        self.assertIsNotNone(session)
        self.assertEqual(session.last_heartbeat_at, later)

    def test_close_session_releases_claims(self):
        now = self._now()
        message_id = str(uuid.uuid4())
        self.store.publish(message_id, "orders", b"hello", now)
        session_id = str(uuid.uuid4())
        self.store.create_session(session_id, "orders", False, now)
        self.store.claim(session_id, now)
        self.store.close_session(session_id)
        message = self.store.find_message(message_id)
        self.assertIsNotNone(message)
        self.assertIsNone(message.claimed_by)
        self.assertIsNone(message.claimed_at)
        self.assertIsNone(self.store.find_session(session_id))

    def test_find_stale_sessions(self):
        now = self._now()
        fresh = now - datetime.timedelta(seconds=1)
        stale = now - datetime.timedelta(seconds=10)
        fresh_id = str(uuid.uuid4())
        self.store.create_session(fresh_id, "orders", False, fresh)
        stale_id = str(uuid.uuid4())
        self.store.create_session(stale_id, "orders", False, stale)
        stale_ids = self.store.find_stale_sessions(now - datetime.timedelta(seconds=5))
        self.assertNotIn(fresh_id, stale_ids)
        self.assertIn(stale_id, stale_ids)

    def test_find_message_nonexistent_returns_none(self):
        self.assertIsNone(self.store.find_message("does-not-exist"))

    def test_find_session_nonexistent_returns_none(self):
        self.assertIsNone(self.store.find_session("does-not-exist"))

    def test_ack_nonexistent_is_noop(self):
        self.store.ack("does-not-exist")

    def test_nack_nonexistent_is_noop(self):
        self.store.nack("does-not-exist")

    def test_heartbeat_nonexistent_is_noop(self):
        self.store.heartbeat("does-not-exist", self._now())

    def test_close_session_nonexistent_is_noop(self):
        self.store.close_session("does-not-exist")


if __name__ == "__main__":
    unittest.main()
