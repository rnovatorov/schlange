import dataclasses
import datetime
import uuid
from typing import List, Optional

from .node import Node
from .node_repository import NodeRepository
from .node_specification import NodeSpecification


@dataclasses.dataclass
class NodeService:

    node_repository: NodeRepository

    def register_node(self, node_id: Optional[str] = None) -> Node:
        """
        Raises:
            IOError: IO error occurred during the operation.
            NodeAlreadyExistsError: Node with this ID already exists.
        """
        if node_id is None:
            node_id = str(uuid.uuid4())
        node = Node.create(now=self._now(), id=node_id)
        self.node_repository.create_node(node)
        return node

    def heartbeat(self, node_id: str) -> Node:
        """
        Raises:
            IOError: IO error occurred during the operation.
            NodeNotFoundError: Node was not found.
        """
        node = self.node_repository.get_node(node_id)
        node.heartbeat(now=self._now())
        self.node_repository.update_node(node)
        return node

    def deregister_node(self, node_id: str) -> None:
        """
        Raises:
            IOError: IO error occurred during the operation.
            NodeNotFoundError: Node was not found.
        """
        self.node_repository.delete_node(node_id)

    def dead_nodes(self, timeout: datetime.timedelta) -> List[Node]:
        """
        Raises:
            IOError: IO error occurred during the operation.
        """
        deadline = self._now() - timeout
        return self.node_repository.list_nodes(
            NodeSpecification(last_heartbeat_before=deadline)
        )

    def _now(self) -> datetime.datetime:
        return datetime.datetime.now(datetime.UTC)
