import dataclasses
import datetime
import pathlib
import tempfile
import unittest

import schlange


class NodeRepositoryTest(unittest.TestCase):

    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        db_path = pathlib.Path(self.dir.name) / "test.db"
        self.db_ctx = schlange.sqlite.Database.open(db_path, read_pool_capacity=4)
        self.db = self.db_ctx.__enter__()
        self.db.migrate()
        self.repo = schlange.sqlite.NodeRepository(self.db)

    def tearDown(self):
        self.db_ctx.__exit__(None, None, None)
        self.dir.cleanup()

    def test_create_and_get_node(self):
        now = datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
        node = schlange.Node.create(now=now, id="node-id")
        self.repo.create_node(node)
        loaded = self.repo.get_node("node-id")
        self.assertEqual(loaded, node)

    def test_create_node_raises_if_already_exists(self):
        now = datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
        node = schlange.Node.create(now=now, id="node-id")
        self.repo.create_node(node)
        with self.assertRaises(schlange.NodeAlreadyExistsError):
            self.repo.create_node(node)

    def test_get_node_raises_if_not_found(self):
        with self.assertRaises(schlange.NodeNotFoundError):
            self.repo.get_node("missing")

    def test_list_nodes(self):
        now = datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
        alive = schlange.Node.create(
            now=now + datetime.timedelta(seconds=20), id="alive"
        )
        dead = schlange.Node.create(now=now, id="dead")
        self.repo.create_node(alive)
        self.repo.create_node(dead)

        nodes = self.repo.list_nodes(
            schlange.NodeSpecification(
                last_heartbeat_before=now + datetime.timedelta(seconds=10)
            )
        )

        self.assertEqual([node.id for node in nodes], ["dead"])

    def test_list_nodes_without_spec(self):
        now = datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
        self.repo.create_node(schlange.Node.create(now=now, id="node-a"))
        self.repo.create_node(schlange.Node.create(now=now, id="node-b"))

        nodes = self.repo.list_nodes(schlange.NodeSpecification())

        self.assertEqual(sorted(node.id for node in nodes), ["node-a", "node-b"])

    def test_delete_node(self):
        now = datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
        self.repo.create_node(schlange.Node.create(now=now, id="node-id"))
        self.repo.delete_node("node-id")
        with self.assertRaises(schlange.NodeNotFoundError):
            self.repo.get_node("node-id")

    def test_delete_node_raises_if_not_found(self):
        with self.assertRaises(schlange.NodeNotFoundError):
            self.repo.delete_node("missing")

    def test_update_node(self):
        now = datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
        node = schlange.Node.create(now=now, id="node-id")
        self.repo.create_node(node)
        node.heartbeat(now=now + datetime.timedelta(seconds=10))
        self.repo.update_node(node)
        loaded = self.repo.get_node("node-id")
        self.assertEqual(loaded.last_heartbeat_at, now + datetime.timedelta(seconds=10))
        self.assertEqual(loaded.version, 2)

    def test_update_node_raises_on_concurrent_modification(self):
        now = datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
        node = schlange.Node.create(now=now, id="node-id")
        self.repo.create_node(node)
        stale = dataclasses.replace(node)
        node.heartbeat(now=now + datetime.timedelta(seconds=10))
        self.repo.update_node(node)
        with self.assertRaises(schlange.NodeUpdatedConcurrentlyError):
            self.repo.update_node(stale)
