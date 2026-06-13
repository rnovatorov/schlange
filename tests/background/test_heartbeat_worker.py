import datetime
import unittest
import uuid
from typing import List
from unittest import mock

import schlange


class FakeNodeService:

    def __init__(self) -> None:
        self.heartbeats: List[str] = []
        self.registered: List[str] = []
        self.deregistered: List[str] = []
        self._should_raise_io_error = False
        self._should_raise_not_found_error = False
        self._should_raise_concurrent_update_error = False
        self._should_raise_not_found_on_deregister = False

    def register_node(self, node_id: str | None = None) -> schlange.Node:
        if node_id is None:
            node_id = str(uuid.uuid4())
        self.registered.append(node_id)
        return schlange.Node.create(now=datetime.datetime.now(datetime.UTC), id=node_id)

    def deregister_node(self, node_id: str) -> None:
        self.deregistered.append(node_id)
        if self._should_raise_not_found_on_deregister:
            raise schlange.NodeNotFoundError()

    def heartbeat(self, node_id: str) -> schlange.Node:
        if self._should_raise_io_error:
            raise IOError("disk full")
        if self._should_raise_not_found_error:
            raise schlange.NodeNotFoundError()
        if self._should_raise_concurrent_update_error:
            raise schlange.NodeUpdatedConcurrentlyError()
        self.heartbeats.append(node_id)
        return schlange.Node.create(now=datetime.datetime.now(datetime.UTC), id=node_id)


class HeartbeatWorkerTest(unittest.TestCase):

    def test_work_calls_heartbeat(self):
        service = FakeNodeService()
        worker = schlange.background.HeartbeatWorker(
            interval=5, node_service=service, node_id="node-id"
        )

        worker.work()
        worker.work()

        self.assertEqual(service.heartbeats, ["node-id", "node-id"])

    def test_work_survives_io_error(self):
        service = FakeNodeService()
        service._should_raise_io_error = True
        worker = schlange.background.HeartbeatWorker(
            interval=5, node_service=service, node_id="node-id"
        )

        worker.work()

        self.assertEqual(service.heartbeats, [])

    def test_work_survives_node_not_found_error(self):
        service = FakeNodeService()
        service._should_raise_not_found_error = True
        worker = schlange.background.HeartbeatWorker(
            interval=5, node_service=service, node_id="node-id"
        )

        worker.work()

        self.assertEqual(service.heartbeats, [])

    def test_work_survives_concurrent_update_error(self):
        service = FakeNodeService()
        service._should_raise_concurrent_update_error = True
        worker = schlange.background.HeartbeatWorker(
            interval=5, node_service=service, node_id="node-id"
        )

        worker.work()

        self.assertEqual(service.heartbeats, [])

    @mock.patch.object(schlange.background.worker.Worker, "start")
    def test_start_registers_node_before_starting_thread(self, worker_start):
        service = FakeNodeService()
        worker = schlange.background.HeartbeatWorker(
            interval=5, node_service=service, node_id="node-id"
        )

        worker.start()

        self.assertEqual(service.registered, ["node-id"])
        worker_start.assert_called_once_with()

    @mock.patch.object(schlange.background.worker.Worker, "start")
    def test_start_generates_node_id_if_not_provided(self, worker_start):
        service = FakeNodeService()
        worker = schlange.background.HeartbeatWorker(
            interval=5, node_service=service, node_id=None
        )

        worker.start()

        self.assertEqual(len(service.registered), 1)
        generated_id = service.registered[0]
        self.assertEqual(worker.node_id, generated_id)
        worker_start.assert_called_once_with()

    @mock.patch.object(schlange.background.worker.Worker, "stop")
    def test_stop_deregisters_node_after_stopping_thread(self, worker_stop):
        service = FakeNodeService()
        worker = schlange.background.HeartbeatWorker(
            interval=5, node_service=service, node_id="node-id"
        )

        worker.stop()

        worker_stop.assert_called_once_with()
        self.assertEqual(service.deregistered, ["node-id"])

    @mock.patch.object(schlange.background.worker.Worker, "stop")
    def test_stop_handles_node_not_found_error(self, worker_stop):
        service = FakeNodeService()
        service._should_raise_not_found_on_deregister = True
        worker = schlange.background.HeartbeatWorker(
            interval=5, node_service=service, node_id="node-id"
        )

        worker.stop()

        worker_stop.assert_called_once_with()
        self.assertEqual(service.deregistered, ["node-id"])
