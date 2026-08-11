import pathlib
import tempfile
import unittest

from schlange.internal import sqlite
from schlange.services.messaging import core
from schlange.services.messaging import sqlite as messaging_sqlite


class ServiceTest(unittest.TestCase):

    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        db_path = pathlib.Path(self.dir.name) / "messaging.db"
        self.db_ctx = sqlite.Database.open(db_path, read_pool_capacity=4)
        self.db = self.db_ctx.__enter__()
        self.db.migrate(migrations=messaging_sqlite.MIGRATIONS)
        store = messaging_sqlite.Store(self.db)
        self.service = core.Service(store=store)

    def tearDown(self):
        self.db_ctx.__exit__(None, None, None)
        self.dir.cleanup()

    def test_declare_queue_duplicate_raises(self):
        self.service.declare_queue("orders", None, 5)
        with self.assertRaises(core.QueueAlreadyExistsError):
            self.service.declare_queue("orders", None, 5)

    def test_declare_queue_unknown_dlq_raises(self):
        with self.assertRaises(core.QueueNotFoundError):
            self.service.declare_queue("payments", "nope", 5)

    def test_publish_message_unknown_queue_raises(self):
        with self.assertRaises(core.QueueNotFoundError):
            self.service.publish_message("nope", b"hello", 30.0)

    def test_claim_message_empty_raises(self):
        self.service.declare_queue("orders", None, 5)
        with self.assertRaises(core.NoMessagesAvailable):
            self.service.claim_message("orders")

    def test_publish_then_claim_roundtrip(self):
        self.service.declare_queue("orders", None, 5)
        message_id = self.service.publish_message("orders", b"hello", 30.0)
        claimed = self.service.claim_message("orders")
        self.assertEqual(claimed.id, message_id)
        self.assertEqual(claimed.payload, b"hello")
        self.assertEqual(claimed.delivery_count, 1)

    def test_requeue_message_routes_to_dlq_at_max_delivery_count(self):
        self.service.declare_queue("orders-dlq", None, 3)
        self.service.declare_queue("orders", "orders-dlq", 2)
        message_id = self.service.publish_message("orders", b"hello", 30.0)
        first = self.service.claim_message("orders")
        self.assertEqual(first.delivery_count, 1)
        self.service.requeue_message(first.id, first.version)
        second = self.service.claim_message("orders")
        self.assertEqual(second.delivery_count, 2)
        self.service.requeue_message(second.id, second.version)
        moved = self.service.find_message(message_id)
        self.assertEqual(moved.queue, "orders-dlq")
        self.assertEqual(moved.payload, b"hello")
        self.assertEqual(moved.delivery_count, 0)
        with self.assertRaises(core.NoMessagesAvailable):
            self.service.claim_message("orders")
        dlq_msg = self.service.claim_message("orders-dlq")
        self.assertEqual(dlq_msg.id, message_id)

    def test_requeue_message_below_max_makes_visible_again(self):
        self.service.declare_queue("orders", None, 5)
        message_id = self.service.publish_message("orders", b"hello", 30.0)
        claimed = self.service.claim_message("orders")
        self.assertEqual(claimed.delivery_count, 1)
        self.service.requeue_message(claimed.id, claimed.version)
        requeued = self.service.claim_message("orders")
        self.assertEqual(requeued.id, message_id)
        self.assertEqual(requeued.delivery_count, 2)

    def test_requeue_message_no_dlq_at_max_drops_message(self):
        self.service.declare_queue("orders", None, 1)
        message_id = self.service.publish_message("orders", b"hello", 30.0)
        claimed = self.service.claim_message("orders")
        self.assertEqual(claimed.delivery_count, 1)
        self.service.requeue_message(claimed.id, claimed.version)
        with self.assertRaises(core.MessageNotFoundError):
            self.service.find_message(message_id)
        with self.assertRaises(core.NoMessagesAvailable):
            self.service.claim_message("orders")


if __name__ == "__main__":
    unittest.main()
