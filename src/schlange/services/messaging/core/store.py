import datetime
from typing import Protocol

from .message import Message
from .queue import Queue


class Store(Protocol):
    """
    Messaging persistence interface.  Each method executes exactly
    one transaction.  Methods that look up an entity by key raise
    on not-found instead of returning None.

    ``now`` is passed in so all time logic lives in the service.
    Operations that take ``version`` perform an optimistic-concurrency
    check: the mutation is a no-op if the stored version does not
    match.
    """

    def declare_queue(
        self,
        name: str,
        dead_letter_queue: str | None,
        visibility_timeout: float,
        now: datetime.datetime,
    ) -> None: ...

    def find_queue(self, name: str) -> Queue: ...

    def publish_message(
        self,
        message_id: str,
        queue: str,
        payload: bytes,
        now: datetime.datetime,
    ) -> None: ...

    def claim_message(
        self,
        queue: str,
        now: datetime.datetime,
    ) -> Message: ...

    def delete_message(self, message_id: str, version: int) -> None: ...

    def move_message_to_dlq(
        self,
        message_id: str,
        version: int,
        dlq: str,
        now: datetime.datetime,
    ) -> None: ...

    def find_message(self, message_id: str) -> Message: ...
