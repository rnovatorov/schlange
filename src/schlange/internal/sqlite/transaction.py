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
