import contextlib
import threading
from typing import Generator, List

from .connection import Connection


class ConnectionPool:

    @classmethod
    @contextlib.contextmanager
    def new(
        cls, url: str, synchronous_full: bool, capacity: int
    ) -> Generator["ConnectionPool", None, None]:
        with contextlib.ExitStack() as stack:
            conns: List[Connection] = []
            for i in range(capacity):
                conn = stack.enter_context(
                    Connection.open(url=url, synchronous_full=synchronous_full)
                )
                conns.append(conn)
            yield cls(conns=conns)

    def __init__(self, conns: List[Connection]) -> None:
        self.semaphore = threading.BoundedSemaphore(value=len(conns))
        self.lock = threading.Lock()
        self.conns = conns

    @contextlib.contextmanager
    def acquire(self) -> Generator[Connection, None, None]:
        with self.semaphore:
            with self.lock:
                conn = self.conns.pop()
            try:
                yield conn
            finally:
                with self.lock:
                    self.conns.append(conn)
