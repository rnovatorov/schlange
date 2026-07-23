import dataclasses
import datetime
import uuid
from typing import Optional

from .message import Message
from .store import Store


@dataclasses.dataclass
class Service:
    """
    Stateless messaging business logic. Owns the clock; delegates
    persistence to a Store.
    """

    store: Store
    session_timeout: float = 5.0

    def publish(self, routing_key: str, payload: bytes) -> str:
        message_id = str(uuid.uuid4())
        self.store.publish(message_id, routing_key, payload, self._now())
        return message_id

    def claim(self, session_id: str) -> Optional[Message]:
        return self.store.claim(session_id, self._now())

    def ack(self, message_id: str) -> None:
        self.store.ack(message_id)

    def nack(self, message_id: str) -> None:
        self.store.nack(message_id)

    def create_session(self, queue: str, dead_letter: bool = False) -> str:
        session_id = str(uuid.uuid4())
        self.store.create_session(session_id, queue, dead_letter, self._now())
        return session_id

    def heartbeat(self, session_id: str) -> None:
        self.store.heartbeat(session_id, self._now())

    def close_session(self, session_id: str) -> None:
        self.store.close_session(session_id)

    def sweep(self) -> None:
        threshold = self._now() - datetime.timedelta(seconds=self.session_timeout)
        stale_session_ids = self.store.find_stale_sessions(threshold)
        for session_id in stale_session_ids:
            self.store.close_session(session_id)

    def _now(self) -> datetime.datetime:
        return datetime.datetime.now(datetime.UTC)
