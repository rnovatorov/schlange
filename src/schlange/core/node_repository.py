from typing import List, Protocol

from .node import Node
from .node_specification import NodeSpecification


class NodeRepository(Protocol):

    def create_node(self, node: Node) -> None:
        pass

    def get_node(self, node_id: str) -> Node:
        pass

    def list_nodes(self, spec: NodeSpecification) -> List[Node]:
        pass

    def delete_node(self, node_id: str) -> None:
        pass

    def update_node(self, node: Node) -> None:
        pass
