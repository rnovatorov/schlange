import sqlite3
from typing import List

from schlange import core

from .data_mapper import DataMapper
from .database import Database
from .errors import NoRowsError

SQL_CREATE_NODE = """
    INSERT INTO nodes (id, version, last_heartbeat_at)
    VALUES (:id, :version, :last_heartbeat_at)
"""

SQL_GET_NODE_BY_ID = """
    SELECT id, version, last_heartbeat_at
    FROM nodes
    WHERE id = :id
"""

SQL_GET_NODES_BY_SPEC = """
    SELECT id, version, last_heartbeat_at
    FROM nodes
    WHERE
        coalesce(last_heartbeat_at < :last_heartbeat_before, true)
"""

SQL_DELETE_NODE_BY_ID = """
    DELETE FROM nodes WHERE id = :id
"""

SQL_UPDATE_NODE_BY_ID = """
    UPDATE nodes
    SET version = :version + 1,
        last_heartbeat_at = :last_heartbeat_at
    WHERE id = :id AND version = :version
"""


class NodeRepository:

    def __init__(self, db: Database) -> None:
        self.db = db
        self.data_mapper = DataMapper()

    def create_node(self, node: core.Node) -> None:
        with self.db.transaction() as tx:
            try:
                tx.execute(
                    SQL_CREATE_NODE,
                    {
                        "id": node.id,
                        "version": node.version,
                        "last_heartbeat_at": self.data_mapper.dump_timestamp(
                            node.last_heartbeat_at
                        ),
                    },
                )
            except sqlite3.IntegrityError:
                raise core.NodeAlreadyExistsError()

    def get_node(self, node_id: str) -> core.Node:
        with self.db.transaction(read_only=True) as tx:
            try:
                row = tx.query_row(SQL_GET_NODE_BY_ID, {"id": node_id})
            except NoRowsError:
                raise core.NodeNotFoundError() from None
            return self._collect_node(row)

    def list_nodes(self, spec: core.NodeSpecification) -> List[core.Node]:
        with self.db.transaction(read_only=True) as tx:
            rows = tx.query(
                SQL_GET_NODES_BY_SPEC,
                {
                    "last_heartbeat_before": (
                        spec.last_heartbeat_before.isoformat()
                        if spec.last_heartbeat_before is not None
                        else None
                    ),
                },
            )
            return [self._collect_node(row) for row in rows]

    def _collect_node(self, row: sqlite3.Row) -> core.Node:
        return core.Node(
            id=row[0],
            version=row[1],
            last_heartbeat_at=self.data_mapper.load_timestamp(row[2]),
        )

    def delete_node(self, node_id: str) -> None:
        with self.db.transaction() as tx:
            rows_affected = tx.execute(SQL_DELETE_NODE_BY_ID, {"id": node_id})
            if not rows_affected:
                raise core.NodeNotFoundError()

    def update_node(self, node: core.Node) -> None:
        with self.db.transaction() as tx:
            rows_affected = tx.execute(
                SQL_UPDATE_NODE_BY_ID,
                {
                    "id": node.id,
                    "version": node.version,
                    "last_heartbeat_at": self.data_mapper.dump_timestamp(
                        node.last_heartbeat_at
                    ),
                },
            )
            if not rows_affected:
                raise core.NodeUpdatedConcurrentlyError()
