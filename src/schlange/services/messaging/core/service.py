import dataclasses
import datetime
import uuid

from .message import Message
from .queue import Queue
from .store import Store


@dataclasses.dataclass
class Service:
    """
    Stateless messaging business logic. Owns the clock and all
    orchestration; delegates single-transaction persistence to a
    Store.
    """

    store: Store

    def declare_queue(
        self,
        name: str,
        dead_letter_queue: str | None,
        visibility_timeout: float,
    ) -> None:
        self.store.declare_queue(
            name, dead_letter_queue, visibility_timeout, self._now()
        )

    def find_queue(self, name: str) -> Queue:
        return self.store.find_queue(name)

    def publish_message(self, queue: str, payload: bytes) -> str:
        message_id = str(uuid.uuid4())
        self.store.publish_message(message_id, queue, payload, self._now())
        return message_id

    def claim_message(self, queue: str) -> Message:
        return self.store.claim_message(queue, self._now())

    def ack_message(self, message_id: str, version: int) -> None:
        self.store.delete_message(message_id, version)

    def nack_message(self, message_id: str, version: int) -> None:
        message = self.store.find_message(message_id)
        queue = self.store.find_queue(message.queue)
        if queue.dead_letter_queue is None:
            self.store.delete_message(message_id, version)
        else:
            self.store.move_message_to_dlq(
                message_id, version, queue.dead_letter_queue, self._now()
            )

    def find_message(self, message_id: str) -> Message:
        return self.store.find_message(message_id)

    def _now(self) -> datetime.datetime:
        return datetime.datetime.now(datetime.UTC)
