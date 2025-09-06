import contextlib
import sqlite3
from typing import Generator

from .errors import NoRowsError


class Transaction:

    @classmethod
    @contextlib.contextmanager
    def begin(
        cls, conn: sqlite3.Connection, read_only: bool
    ) -> Generator["Transaction", None, None]:
        mode = "IMMEDIATE"
        if read_only:
            mode = "DEFERRED"
        conn.execute(f"BEGIN {mode}")
        try:
            yield Transaction(cursor=conn.cursor())
        except:
            conn.rollback()
            raise
        else:
            conn.commit()

    @classmethod
    @contextlib.contextmanager
    def begin_with_script(
        cls, conn: sqlite3.Connection, script: str
    ) -> Generator["Transaction", None, None]:
        # NOTE: `sqlite3.Cursor.executescript` makes an implicit COMMIT before
        # executing the script if the autocommit is LEGACY_TRANSACTION_CONTROL
        # and there is a pending transaction. Fortunately it does not
        # implicitly COMMIT after executing the script so we can start a
        # transaction inside the script and ensure we atomically execute it and
        # do whatever else is needed later in the same transaction.
        #
        # See:
        # - https://docs.python.org/3/library/sqlite3.html#sqlite3.Cursor.executescript
        with cls.begin(conn=conn, read_only=False) as tx:
            tx.cursor.executescript("BEGIN IMMEDIATE; " + script)
            yield tx

    def __init__(self, cursor: sqlite3.Cursor) -> None:
        self.cursor = cursor

    def execute(self, sql: str, *args, **kwargs) -> int:
        self.cursor.execute(sql, *args, **kwargs)
        return self.cursor.rowcount

    def query_row(self, sql: str, *args, **kwargs) -> sqlite3.Row:
        self.cursor.execute(sql, *args, **kwargs)
        row = self.cursor.fetchone()
        if row is None:
            raise NoRowsError()
        return row

    def query(self, sql: str, *args, **kwargs) -> Generator[sqlite3.Row, None, None]:
        self.cursor.execute(sql, *args, **kwargs)
        while True:
            rows = self.cursor.fetchmany()
            if not rows:
                return
            for row in rows:
                yield row
