import dataclasses
import datetime
import unittest

import schlange


class NodeTest(unittest.TestCase):

    def test_create(self):
        now = datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
        node = schlange.Node.create(now=now, id="node-id")
        self.assertEqual(node.id, "node-id")
        self.assertEqual(node.version, 1)
        self.assertEqual(node.last_heartbeat_at, now)

    def test_heartbeat(self):
        now = datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
        node = schlange.Node.create(now=now, id="node-id")
        later = now + datetime.timedelta(seconds=30)
        node.heartbeat(now=later)
        self.assertEqual(node.last_heartbeat_at, later)

    def test_heartbeat_does_not_change_version(self):
        now = datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
        node = schlange.Node.create(now=now, id="node-id")
        node.heartbeat(now=now + datetime.timedelta(seconds=1))
        self.assertEqual(node.version, 1)

    def test_nodes_with_equal_fields_are_equal(self):
        now = datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
        node_a = schlange.Node.create(now=now, id="node-id")
        node_b = schlange.Node.create(now=now, id="node-id")
        self.assertEqual(node_a, node_b)

    def test_nodes_with_different_last_heartbeat_are_not_equal(self):
        now = datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
        node_a = schlange.Node.create(now=now, id="node-id")
        node_b = schlange.Node.create(
            now=now + datetime.timedelta(seconds=1), id="node-id"
        )
        self.assertNotEqual(node_a, node_b)
