import contextlib
import logging
import pathlib
from typing import Generator

from .connection import Connection
from .connection_pool import ConnectionPool
from .migration import Migration
from .transaction import Transaction

LOGGER = logging.getLogger(__name__)


SQL_CREATE_SCHEMA_VERSION_TABLE = """
    CREATE TABLE IF NOT EXISTS schema_version (
        version INTEGER NOT NULL
    )
"""

SQL_SET_DEFAULT_SCHEMA_VERSION = """
    INSERT INTO schema_version (version)
    SELECT 0
    WHERE NOT EXISTS (SELECT 1 FROM schema_version)
"""


SQL_SELECT_CURRENT_SCHEMA_VERSION = """
    SELECT version
    FROM schema_version
"""

SQL_UPDATE_CURRENT_SCHEMA_VERSION = """
    UPDATE schema_version
    SET version = :version
"""


class Database:

    @classmethod
    @contextlib.contextmanager
    def open(
        cls,
        path: pathlib.Path,
        read_pool_capacity: int,
        write_pool_capacity: int = 1,
        sync_write_pool_capacity: int = 1,
    ) -> Generator["Database", None, None]:
        with contextlib.ExitStack() as stack:
            read_pool = stack.enter_context(
                ConnectionPool.new(
                    path=path,
                    synchronous_full=False,
                    capacity=read_pool_capacity,
                )
            )
            write_pool = stack.enter_context(
                ConnectionPool.new(
                    path=path,
                    synchronous_full=False,
                    capacity=write_pool_capacity,
                )
            )
            sync_write_pool = stack.enter_context(
                ConnectionPool.new(
                    path=path,
                    synchronous_full=True,
                    capacity=sync_write_pool_capacity,
                )
            )
            yield cls(
                read_pool=read_pool,
                write_pool=write_pool,
                sync_write_pool=sync_write_pool,
            )

    def __init__(
        self,
        read_pool: ConnectionPool,
        write_pool: ConnectionPool,
        sync_write_pool: ConnectionPool,
    ) -> None:
        self.read_pool = read_pool
        self.write_pool = write_pool
        self.sync_write_pool = sync_write_pool

    @contextlib.contextmanager
    def transaction(
        self, read_only: bool = False, synchronous: bool = True
    ) -> Generator[Transaction, None, None]:
        pool = (
            self.read_pool
            if read_only
            else self.sync_write_pool if synchronous else self.write_pool
        )
        with pool.acquire() as conn:
            with conn.transaction(read_only=read_only) as tx:
                yield tx

    def migrate(self, migrations: list[Migration]) -> None:
        with self.write_pool.acquire() as conn:
            self._migrate(conn, migrations)

    def _migrate(self, conn: Connection, migrations: list[Migration]) -> None:
        self._ensure_schema_version_table(conn)
        for version, migration in enumerate(migrations, start=1):
            self._apply_migration(conn, migration, version)

    def _ensure_schema_version_table(self, conn: Connection) -> None:
        with conn.transaction() as tx:
            tx.execute(SQL_CREATE_SCHEMA_VERSION_TABLE)
            tx.execute(SQL_SET_DEFAULT_SCHEMA_VERSION)

    def _apply_migration(
        self, conn: Connection, migration: Migration, version: int
    ) -> None:
        # Re-check the version under the write lock: another process may
        # have applied this migration while we waited for it. The check,
        # the body, and the version bump share one transaction, so the body
        # runs at most once even under concurrent cold starts. This relies
        # on statements being issued one at a time via execute() -- never
        # executescript(), which implicitly commits the surrounding
        # transaction and would break the atomicity of the check-then-act.
        with conn.transaction() as tx:
            current = tx.query_row(SQL_SELECT_CURRENT_SCHEMA_VERSION)[0]
            if current >= version:
                return
            for statement in migration.statements:
                tx.execute(statement)
            tx.execute(SQL_UPDATE_CURRENT_SCHEMA_VERSION, {"version": version})
            LOGGER.info("migrated database to version %d", version)
