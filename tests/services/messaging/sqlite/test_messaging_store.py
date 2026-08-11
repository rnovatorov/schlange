import datetime
import pathlib
import tempfile
import unittest
import uuid

from schlange.internal import sqlite
from schlange.services.messaging import core
from schlange.services.messaging import sqlite as messaging_sqlite


class StoreTest(unittest.TestCase):

    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        db_path = pathlib.Path(self.dir.name) / "messaging.db"
        self.db_ctx = sqlite.Database.open(db_path, read_pool_capacity=4)
        self.db = self.db_ctx.__enter__()
        self.db.migrate(migrations=messaging_sqlite.MIGRATIONS)
        self.store = messaging_sqlite.Store(self.db)
        self.now = self._now()
        self.store.create_queue("orders", None, 5, self.now)

    def tearDown(self):
        self.db_ctx.__exit__(None, None, None)
        self.dir.cleanup()

    def _now(self) -> datetime.datetime:
        return datetime.datetime.now(datetime.UTC)

    def test_create_queue(self):
        q = self.store.find_queue("orders")
        self.assertEqual(q.name, "orders")
        self.assertIsNone(q.dead_letter_queue)
        self.assertEqual(q.max_delivery_count, 5)

    def test_create_queue_with_dlq(self):
        self.store.create_queue("orders-dlq", None, 3, self.now)
        self.store.create_queue("payments", "orders-dlq", 10, self.now)
        q = self.store.find_queue("payments")
        self.assertEqual(q.dead_letter_queue, "orders-dlq")
        self.assertEqual(q.max_delivery_count, 10)

    def test_create_queue_duplicate_raises(self):
        with self.assertRaises(core.QueueAlreadyExistsError):
            self.store.create_queue("orders", None, 5, self.now)

    def test_create_queue_unknown_dlq_raises(self):
        with self.assertRaises(core.QueueNotFoundError):
            self.store.create_queue("payments", "nope", 5, self.now)

    def test_find_queue_not_found_raises(self):
        with self.assertRaises(core.QueueNotFoundError):
            self.store.find_queue("nope")

    def test_publish_message(self):
        message_id = str(uuid.uuid4())
        self.store.publish_message(message_id, "orders", b"hello", 30.0, self.now)
        message = self.store.find_message(message_id)
        self.assertEqual(message.id, message_id)
        self.assertEqual(message.queue, "orders")
        self.assertEqual(message.payload, b"hello")
        self.assertEqual(message.visibility_timeout, 30.0)
        self.assertEqual(message.delivery_count, 0)
        self.assertEqual(message.version, 0)

    def test_publish_message_unknown_queue_raises(self):
        with self.assertRaises(core.QueueNotFoundError):
            self.store.publish_message(
                str(uuid.uuid4()), "nope", b"hello", 30.0, self.now
            )

    def test_claim_message_available(self):
        message_id = str(uuid.uuid4())
        self.store.publish_message(message_id, "orders", b"hello", 30.0, self.now)
        message = self.store.claim_message("orders", self.now)
        self.assertEqual(message.id, message_id)
        self.assertEqual(message.version, 1)
        self.assertEqual(message.delivery_count, 1)

    def test_claim_message_empty_raises(self):
        with self.assertRaises(core.NoMessagesAvailable):
            self.store.claim_message("orders", self.now)

    def test_claim_message_unknown_queue_raises(self):
        with self.assertRaises(core.NoMessagesAvailable):
            self.store.claim_message("nope", self.now)

    def test_claim_message_fifo(self):
        first = self.now
        second = self.now + datetime.timedelta(seconds=1)
        id_first = str(uuid.uuid4())
        self.store.publish_message(id_first, "orders", b"one", 30.0, first)
        id_second = str(uuid.uuid4())
        self.store.publish_message(id_second, "orders", b"two", 30.0, second)
        claimed_first = self.store.claim_message("orders", second)
        claimed_second = self.store.claim_message("orders", second)
        self.assertEqual(claimed_first.id, id_first)
        self.assertEqual(claimed_second.id, id_second)

    def test_claim_message_skips_invisible(self):
        self.store.publish_message(
            str(uuid.uuid4()), "orders", b"hello", 30.0, self.now
        )
        self.store.claim_message("orders", self.now)
        with self.assertRaises(core.NoMessagesAvailable):
            self.store.claim_message("orders", self.now)

    def test_claim_message_reclaims_after_timeout(self):
        message_id = str(uuid.uuid4())
        self.store.publish_message(message_id, "orders", b"hello", 30.0, self.now)
        first = self.store.claim_message("orders", self.now)
        self.assertEqual(first.version, 1)
        self.assertEqual(first.delivery_count, 1)
        later = self.now + datetime.timedelta(seconds=31)
        second = self.store.claim_message("orders", later)
        self.assertEqual(second.id, message_id)
        self.assertEqual(second.version, 2)
        self.assertEqual(second.delivery_count, 2)

    def test_claim_message_uses_per_message_visibility_timeout(self):
        message_id = str(uuid.uuid4())
        self.store.publish_message(message_id, "orders", b"hello", 10.0, self.now)
        first = self.store.claim_message("orders", self.now)
        self.assertEqual(first.version, 1)
        before_timeout = self.now + datetime.timedelta(seconds=9)
        with self.assertRaises(core.NoMessagesAvailable):
            self.store.claim_message("orders", before_timeout)
        after_timeout = self.now + datetime.timedelta(seconds=10)
        second = self.store.claim_message("orders", after_timeout)
        self.assertEqual(second.id, message_id)

    def test_claim_message_delivery_count_increments(self):
        message_id = str(uuid.uuid4())
        self.store.publish_message(message_id, "orders", b"hello", 1.0, self.now)
        first = self.store.claim_message("orders", self.now)
        self.assertEqual(first.delivery_count, 1)
        second = self.store.claim_message(
            "orders", self.now + datetime.timedelta(seconds=1)
        )
        self.assertEqual(second.delivery_count, 2)
        third = self.store.claim_message(
            "orders", self.now + datetime.timedelta(seconds=2)
        )
        self.assertEqual(third.delivery_count, 3)

    def test_claim_message_competing_consumers(self):
        message_id = str(uuid.uuid4())
        self.store.publish_message(message_id, "orders", b"hello", 30.0, self.now)
        claimed_a = self.store.claim_message("orders", self.now)
        with self.assertRaises(core.NoMessagesAvailable):
            self.store.claim_message("orders", self.now)
        self.assertEqual(claimed_a.id, message_id)

    def test_delete_message(self):
        message_id = str(uuid.uuid4())
        self.store.publish_message(message_id, "orders", b"hello", 30.0, self.now)
        claimed = self.store.claim_message("orders", self.now)
        self.store.delete_message(claimed.id, claimed.version)
        with self.assertRaises(core.MessageNotFoundError):
            self.store.find_message(message_id)

    def test_delete_message_wrong_version_is_noop(self):
        message_id = str(uuid.uuid4())
        self.store.publish_message(message_id, "orders", b"hello", 30.0, self.now)
        claimed = self.store.claim_message("orders", self.now)
        self.store.delete_message(claimed.id, claimed.version + 999)
        message = self.store.find_message(message_id)
        self.assertEqual(message.version, claimed.version)

    def test_delete_message_nonexistent_is_noop(self):
        self.store.delete_message("does-not-exist", 0)

    def test_requeue_message_makes_visible_again(self):
        message_id = str(uuid.uuid4())
        self.store.publish_message(message_id, "orders", b"hello", 30.0, self.now)
        claimed = self.store.claim_message("orders", self.now)
        self.store.requeue_message(claimed.id, claimed.version, self.now)
        requeued = self.store.claim_message("orders", self.now)
        self.assertEqual(requeued.id, message_id)
        self.assertEqual(requeued.delivery_count, claimed.delivery_count + 1)

    def test_requeue_message_wrong_version_is_noop(self):
        message_id = str(uuid.uuid4())
        self.store.publish_message(message_id, "orders", b"hello", 30.0, self.now)
        claimed = self.store.claim_message("orders", self.now)
        self.store.requeue_message(claimed.id, claimed.version + 999, self.now)
        with self.assertRaises(core.NoMessagesAvailable):
            self.store.claim_message("orders", self.now)

    def test_move_message_to_dlq_preserves_payload_resets_delivery_count(self):
        self.store.create_queue("orders-dlq", None, 3, self.now)
        message_id = str(uuid.uuid4())
        self.store.publish_message(message_id, "orders", b"hello", 30.0, self.now)
        claimed = self.store.claim_message("orders", self.now)
        self.store.move_message_to_dlq(
            claimed.id, claimed.version, "orders-dlq", self.now
        )
        message = self.store.find_message(message_id)
        self.assertEqual(message.queue, "orders-dlq")
        self.assertEqual(message.payload, b"hello")
        self.assertEqual(message.delivery_count, 0)
        self.assertEqual(message.version, claimed.version + 1)
        dlq_msg = self.store.claim_message("orders-dlq", self.now)
        self.assertEqual(dlq_msg.id, message_id)
        self.assertEqual(dlq_msg.delivery_count, 1)

    def test_move_message_to_dlq_wrong_version_is_noop(self):
        self.store.create_queue("orders-dlq", None, 3, self.now)
        message_id = str(uuid.uuid4())
        self.store.publish_message(message_id, "orders", b"hello", 30.0, self.now)
        claimed = self.store.claim_message("orders", self.now)
        self.store.move_message_to_dlq(
            claimed.id, claimed.version + 999, "orders-dlq", self.now
        )
        message = self.store.find_message(message_id)
        self.assertEqual(message.queue, "orders")

    def test_find_message_not_found_raises(self):
        with self.assertRaises(core.MessageNotFoundError):
            self.store.find_message("does-not-exist")


if __name__ == "__main__":
    unittest.main()
