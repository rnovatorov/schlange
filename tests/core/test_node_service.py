import dataclasses
import datetime
import unittest
import uuid
from typing import List

import schlange


class FakeNodeRepository:

    def __init__(self) -> None:
        self.nodes: dict[str, schlange.Node] = {}
        self._should_raise_io_error = False

    def create_node(self, node: schlange.Node) -> None:
        if self._should_raise_io_error:
            raise IOError("disk full")
        if node.id in self.nodes:
            raise schlange.NodeAlreadyExistsError()
        self.nodes[node.id] = dataclasses.replace(node)

    def get_node(self, node_id: str) -> schlange.Node:
        if self._should_raise_io_error:
            raise IOError("disk full")
        try:
            return self.nodes[node_id]
        except KeyError:
            raise schlange.NodeNotFoundError()

    def list_nodes(self, spec: schlange.NodeSpecification) -> List[schlange.Node]:
        if self._should_raise_io_error:
            raise IOError("disk full")
        return [node for node in self.nodes.values() if spec.is_satisfied_by(node)]

    def delete_node(self, node_id: str) -> None:
        if self._should_raise_io_error:
            raise IOError("disk full")
        if node_id not in self.nodes:
            raise schlange.NodeNotFoundError()
        del self.nodes[node_id]

    def update_node(self, node: schlange.Node) -> None:
        if self._should_raise_io_error:
            raise IOError("disk full")
        if node.id not in self.nodes:
            raise schlange.NodeUpdatedConcurrentlyError()
        stored = self.nodes[node.id]
        if stored.version != node.version:
            raise schlange.NodeUpdatedConcurrentlyError()
        self.nodes[node.id] = dataclasses.replace(node, version=node.version + 1)


class NodeServiceTest(unittest.TestCase):

    def setUp(self):
        self.repo = FakeNodeRepository()
        self.service = schlange.NodeService(node_repository=self.repo)

    def test_register_node_generates_id_if_not_provided(self):
        now = datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
        self.service._now = lambda: now

        node = self.service.register_node()

        uuid.UUID(node.id)
        self.assertEqual(node.version, 1)
        self.assertEqual(node.last_heartbeat_at, now)
        self.assertEqual(self.repo.nodes[node.id], node)

    def test_register_node_uses_provided_id(self):
        now = datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
        self.service._now = lambda: now

        node = self.service.register_node("node-id")

        self.assertEqual(node.id, "node-id")
        self.assertEqual(node.version, 1)
        self.assertEqual(node.last_heartbeat_at, now)
        self.assertEqual(self.repo.nodes["node-id"], node)

    def test_register_node_raises_if_already_exists(self):
        self.service.register_node("node-id")
        with self.assertRaises(schlange.NodeAlreadyExistsError):
            self.service.register_node("node-id")

    def test_heartbeat(self):
        now = datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
        self.service._now = lambda: now
        self.service.register_node("node-id")
        later = now + datetime.timedelta(seconds=30)
        self.service._now = lambda: later

        node = self.service.heartbeat("node-id")

        self.assertEqual(node.last_heartbeat_at, later)
        stored = self.repo.nodes["node-id"]
        self.assertEqual(stored.last_heartbeat_at, later)
        self.assertEqual(stored.version, 2)

    def test_heartbeat_raises_if_node_not_found(self):
        with self.assertRaises(schlange.NodeNotFoundError):
            self.service.heartbeat("missing")

    def test_deregister_node(self):
        self.service.register_node("node-id")
        self.service.deregister_node("node-id")
        self.assertNotIn("node-id", self.repo.nodes)

    def test_deregister_node_raises_if_node_not_found(self):
        with self.assertRaises(schlange.NodeNotFoundError):
            self.service.deregister_node("missing")

    def test_dead_nodes(self):
        now = datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
        self.service._now = lambda: now
        self.service.register_node("alive")
        self.service.register_node("dead")
        self.service._now = lambda: now + datetime.timedelta(seconds=40)
        self.service.heartbeat("alive")
        self.service._now = lambda: now + datetime.timedelta(seconds=60)

        dead_nodes = self.service.dead_nodes(timeout=datetime.timedelta(seconds=30))

        self.assertEqual([node.id for node in dead_nodes], ["dead"])

    def test_dead_nodes_returns_empty_list_when_all_alive(self):
        now = datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
        self.service._now = lambda: now
        self.service.register_node("node-a")
        self.service.register_node("node-b")
        self.service._now = lambda: now + datetime.timedelta(seconds=10)

        dead_nodes = self.service.dead_nodes(timeout=datetime.timedelta(seconds=30))

        self.assertEqual(dead_nodes, [])
