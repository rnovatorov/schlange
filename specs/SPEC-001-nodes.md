# SPEC-001: Nodes

## Context

Schlange currently supports concurrency only within a single process. To enable
multi-process execution, each Schlange instance needs a unique identity and a
way to signal liveness to other instances sharing the same database.

This spec introduces the node concept — a pure addition with no changes to
existing task or schedule behavior.

## Requirements

### 1. Node Domain Model

1.1. Add `core.Node` dataclass:
```python
@dataclasses.dataclass
class Node(Aggregate):
    last_heartbeat_at: datetime.datetime

    @classmethod
    def create(cls, now: datetime.datetime, id: str) -> "Node":
        return cls(id=id, version=1, last_heartbeat_at=now)

    def heartbeat(self, now: datetime.datetime) -> None:
        self.last_heartbeat_at = now
```
Inherits `id: str` and `version: int` from `Aggregate`.

1.2. Add `core.NodeSpecification` dataclass:
```python
@dataclasses.dataclass
class NodeSpecification:
    last_heartbeat_before: Optional[datetime.datetime] = None

    def is_satisfied_by(self, node: Node) -> bool:
        preconditions = [
            (
                self.last_heartbeat_before is None
                or node.last_heartbeat_at < self.last_heartbeat_before
            ),
        ]
        return all(preconditions)
```

1.3. Add `core.NodeRepository` protocol (CRUD):
```python
class NodeRepository(Protocol):
    def create_node(self, node: Node) -> None: ...
    def get_node(self, node_id: str) -> Node: ...
    def list_nodes(self, spec: NodeSpecification) -> List[Node]: ...
    def delete_node(self, node_id: str) -> None: ...
    def update_node(self, node: Node) -> None: ...
```

1.4. Add `core.NodeService` dataclass:
```python
@dataclasses.dataclass
class NodeService:
    node_repository: NodeRepository

    def register_node(self, node_id: str) -> Node:
        """
        Raises:
            IOError: IO error occurred during the operation.
            NodeAlreadyExistsError: Node with this ID already exists.
        """
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
```

1.5. Add error types to `core.errors`:
- `NodeAlreadyExistsError(Error)`
- `NodeNotFoundError(Error)`

1.6. Export `Node`, `NodeSpecification`, `NodeRepository`, `NodeService`,
`NodeAlreadyExistsError`, `NodeNotFoundError` from `core.__init__`.

### 2. SQLite Node Repository

2.1. Add `sqlite.NodeRepository` class implementing `core.NodeRepository`.

2.2. SQL queries:
```sql
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
```

2.3. Implementation:
- `create_node`: INSERT, raise `NodeAlreadyExistsError` on `IntegrityError`
- `get_node`: SELECT by id, raise `NodeNotFoundError` on `NoRowsError`
- `list_nodes`: SELECT with `NodeSpecification` filters
- `delete_node`: DELETE by id, raise `NodeNotFoundError` if 0 rows
- `update_node`: UPDATE by id with version check, raise
  `NodeUpdatedConcurrentlyError` if 0 rows

2.4. Add `_collect_node` helper to map rows to `core.Node`.

2.5. Export `NodeRepository` from `sqlite.__init__`.

2.6. Add `NodeUpdatedConcurrentlyError(Error)` to `core.errors` and export.

### 3. Schema Migration

3.1. Add `src/schlange/sqlite/migrations/03_add_nodes.sql`:
```sql
CREATE TABLE nodes (
    id TEXT PRIMARY KEY,
    version INTEGER NOT NULL,
    last_heartbeat_at TEXT NOT NULL
);
```

### 4. HeartbeatWorker

4.1. Add `background.HeartbeatWorker` extending `Worker`:
```python
class HeartbeatWorker(Worker):
    def __init__(self, interval: float, node_service: core.NodeService, node_id: str):
        super().__init__(name="schlange.HeartbeatWorker", interval=interval)
        self.node_service = node_service
        self.node_id = node_id

    def start(self) -> None:
        self.node_service.register_node(self.node_id)
        super().start()

    def stop(self) -> None:
        super().stop()
        try:
            self.node_service.deregister_node(self.node_id)
        except core.NodeNotFoundError:
            pass

    def work(self) -> None:
        try:
            self.node_service.heartbeat(self.node_id)
        except (IOError, core.NodeNotFoundError, core.NodeUpdatedConcurrentlyError) as err:
            LOGGER.error("failed to heartbeat: node_id=%s, err=%r",
                         self.node_id, err)
```

4.2. Export `HeartbeatWorker` from `background.__init__`.

### 5. Schlange Integration

5.1. Add top-level module constant in `schlange.py`:
```python
DEFAULT_HEARTBEAT_WORKER_INTERVAL = 5
```

5.2. Add `node_id: str` and `heartbeat_worker: background.HeartbeatWorker`
fields to `Schlange` dataclass.

5.3. Add parameters to `Schlange.new()`:
- `node_id: Optional[str] = None` — defaults to `str(uuid.uuid4())`
- `heartbeat_worker_interval: float = DEFAULT_HEARTBEAT_WORKER_INTERVAL`

5.4. In `Schlange.new()`:
- Create `NodeService` with `sqlite.NodeRepository(db)`
- Create `HeartbeatWorker` (which will handle registration/deregistration)

5.5. `Schlange.start()` starts `heartbeat_worker` (which registers the node).

5.6. `Schlange.stop()` stops `heartbeat_worker` (which deregisters the node).

5.7. Update `calculate_optimal_database_read_pool_capacity` to add +1 for the
heartbeat worker.

### 6. Data Mapper

6.1. Add `load_node` and `dump_node` methods to `sqlite.DataMapper` (or handle
inline in `NodeRepository` — follow existing patterns).

## Constraints

- **Pure addition**: No changes to existing task or schedule behavior.
- **Backward compatibility**: Existing single-process usage works unchanged.
- **SQLite only**: No external dependencies.
- **Existing tests must pass**: No breaking changes.
- **Code style**: Follow existing conventions (black, isort, mypy).

## Acceptance Criteria

1. A Schlange instance registers itself as a node on startup.
2. The heartbeat worker periodically updates `last_heartbeat_at`.
3. On graceful shutdown, the node is deregistered.
4. `NodeService.dead_nodes(timeout)` returns nodes whose heartbeat is older
   than the timeout.
5. All existing tests pass.
6. New tests cover:
   - `NodeRepository`: create, get, list, delete, update
   - `NodeService`: register, heartbeat, deregister, dead_nodes
   - `HeartbeatWorker`: calls heartbeat on each tick
   - Integration: Schlange registers on start, deregisters on stop
7. `make test` passes.
8. `make lint` passes.
