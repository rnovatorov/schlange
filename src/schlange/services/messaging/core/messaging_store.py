import datetime
from typing import List, Optional, Protocol

from .message import Message
from .session import Session


class MessagingStore(Protocol):
    """
    Messaging persistence interface. `now` is passed in so all time
    logic lives in the service; the caller supplies the id for
    publish/create_session. claim returns the claimed Message (None
    when no available messages). claim derives the queue and
    dead-letter flag from the session.
    """

    def publish(
        self,
        message_id: str,
        routing_key: str,
        payload: bytes,
        now: datetime.datetime,
    ) -> None: ...

    def claim(
        self,
        session_id: str,
        now: datetime.datetime,
    ) -> Optional[Message]: ...

    def ack(self, message_id: str) -> None: ...

    def nack(self, message_id: str) -> None: ...

    def create_session(
        self,
        session_id: str,
        queue: str,
        dead_letter: bool,
        now: datetime.datetime,
    ) -> None: ...

    def heartbeat(self, session_id: str, now: datetime.datetime) -> None: ...

    def close_session(self, session_id: str) -> None: ...

    def find_stale_sessions(self, threshold: datetime.datetime) -> List[str]: ...

    def find_message(self, message_id: str) -> Optional[Message]: ...

    def find_session(self, session_id: str) -> Optional[Session]: ...
