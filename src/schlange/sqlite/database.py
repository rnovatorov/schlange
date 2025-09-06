import contextlib
import logging
import pathlib
import sqlite3
from typing import Generator

from .connection import Connection
from .connection_pool import ConnectionPool
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

READ_POOL_CAPACITY = 4
DATABASE_MIGRATIONS_PATH = pathlib.Path(__file__).parent / "migrations"


class Database:

    @classmethod
    @contextlib.contextmanager
    def open(cls, url: str) -> Generator["Database", None, None]:
        with contextlib.ExitStack() as stack:
            read_pool = stack.enter_context(
                ConnectionPool.new(
                    url=url,
                    synchronous_full=False,
                    capacity=READ_POOL_CAPACITY,
                )
            )
            write_pool = stack.enter_context(
                ConnectionPool.new(
                    url=url,
                    synchronous_full=False,
                    capacity=1,
                )
            )
            sync_write_pool = stack.enter_context(
                ConnectionPool.new(
                    url=url,
                    synchronous_full=True,
                    capacity=1,
                )
            )
            yield cls(
                read_pool=read_pool,
                write_pool=write_pool,
                sync_write_pool=sync_write_pool,
                migrations_path=DATABASE_MIGRATIONS_PATH,
            )

    def __init__(
        self,
        read_pool: ConnectionPool,
        write_pool: ConnectionPool,
        sync_write_pool: ConnectionPool,
        migrations_path: pathlib.Path,
    ) -> None:
        self.read_pool = read_pool
        self.write_pool = write_pool
        self.sync_write_pool = sync_write_pool
        self.migrations_path = migrations_path

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

    def migrate(self) -> None:
        if self.migrations_path is None:
            raise FileNotFoundError()
        with self.write_pool.acquire() as conn:
            self._migrate(conn)

    def _migrate(self, conn: Connection) -> None:
        with conn.transaction() as tx:
            tx.execute(SQL_CREATE_SCHEMA_VERSION_TABLE)
            tx.execute(SQL_SET_DEFAULT_SCHEMA_VERSION)
            schema_version = tx.query_row(SQL_SELECT_CURRENT_SCHEMA_VERSION)[0]

        scripts = []
        assert self.migrations_path is not None
        for path in self.migrations_path.glob("*_*.sql"):
            version = int(path.name.split("_", maxsplit=1)[0])
            if version > schema_version:
                scripts.append((version, path))
        scripts.sort(key=lambda version_and_path: version_and_path[0])

        for version, path in scripts:
            try:
                self._execute_migration_script(conn, version, path)
                LOGGER.info("migrated database to version %d", version)
            except sqlite3.Error:
                LOGGER.error("failed to migrate database to version %d", version)
                raise

    def _execute_migration_script(
        self, conn: Connection, version: int, path: pathlib.Path
    ) -> None:
        script = path.read_text()
        with conn.transaction_with_script(script) as tx:
            tx.execute(SQL_UPDATE_CURRENT_SCHEMA_VERSION, {"version": version})
