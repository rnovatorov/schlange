import contextlib
import sqlite3
from typing import ContextManager, Generator

from .transaction import Transaction


class Connection:

    @classmethod
    @contextlib.contextmanager
    def open(
        cls, url: str, synchronous_full: bool
    ) -> Generator["Connection", None, None]:
        conn = sqlite3.connect(url, isolation_level=None, check_same_thread=False)
        try:
            # See: https://www.sqlite.org/pragma.html#pragma_journal_mode
            conn.execute("PRAGMA journal_mode = WAL")
            # See: https://www.sqlite.org/pragma.html#pragma_synchronous
            if not synchronous_full:
                conn.execute("PRAGMA synchronous = NORMAL")
            yield cls(conn=conn)
        finally:
            conn.close()

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def transaction(self, read_only: bool = False) -> ContextManager[Transaction]:
        return Transaction.begin(conn=self.conn, read_only=read_only)

    def transaction_with_script(self, script: str) -> ContextManager[Transaction]:
        return Transaction.begin_with_script(conn=self.conn, script=script)
