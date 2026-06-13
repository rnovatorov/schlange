import logging
from typing import Optional

from schlange import core

from .worker import Worker

LOGGER = logging.getLogger(__name__)


class HeartbeatWorker(Worker):

    def __init__(
        self, interval: float, node_service: core.NodeService, node_id: Optional[str]
    ) -> None:
        super().__init__(name="schlange.HeartbeatWorker", interval=interval)
        self.node_service = node_service
        self.node_id = node_id

    def start(self) -> None:
        node = self.node_service.register_node(self.node_id)
        self.node_id = node.id
        super().start()

    def stop(self) -> None:
        super().stop()
        assert self.node_id is not None
        try:
            self.node_service.deregister_node(self.node_id)
        except core.NodeNotFoundError:
            pass

    def work(self) -> None:
        assert self.node_id is not None
        try:
            self.node_service.heartbeat(self.node_id)
        except (
            IOError,
            core.NodeNotFoundError,
            core.NodeUpdatedConcurrentlyError,
        ) as err:
            LOGGER.error("failed to heartbeat: node_id=%s, err=%r", self.node_id, err)
