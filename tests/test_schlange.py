import pathlib
import tempfile
import unittest

import schlange


class SchlangeTest(unittest.TestCase):

    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.db_path = pathlib.Path(self.dir.name) / "test.db"

    def tearDown(self):
        self.dir.cleanup()

    def test_register_on_start_and_deregister_on_stop(self):
        with schlange.Schlange.new(
            database_path=self.db_path,
            node_id="node-id",
            heartbeat_worker_interval=5,
        ) as queue:
            self.assertEqual(queue.node_id, "node-id")
            # Node is not registered yet
            with self.assertRaises(schlange.NodeNotFoundError):
                queue.node_service.node_repository.get_node("node-id")

            with queue:
                # Node is now registered
                node = queue.node_service.node_repository.get_node("node-id")
                self.assertEqual(node.id, "node-id")
                self.assertEqual(node.version, 1)

            # Node is deregistered after stop
            with self.assertRaises(schlange.NodeNotFoundError):
                queue.node_service.node_repository.get_node("node-id")
