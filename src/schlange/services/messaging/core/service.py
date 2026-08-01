import dataclasses
import datetime
import uuid

from .message import Message
from .store import Store


@dataclasses.dataclass
class Service:

    store: Store

    def declare_queue(
        self,
        name: str,
        dead_letter_queue: str | None,
        max_delivery_count: int,
    ) -> None:
        self.store.create_queue(
            name, dead_letter_queue, max_delivery_count, self._now()
        )

    def find_queue(self, name: str):
        return self.store.find_queue(name)

    def publish_message(
        self,
        queue: str,
        payload: bytes,
        visibility_timeout: float,
    ) -> str:
        message_id = str(uuid.uuid4())
        self.store.publish_message(
            message_id, queue, payload, visibility_timeout, self._now()
        )
        return message_id

    def claim_message(self, queue: str) -> Message:
        return self.store.claim_message(queue, self._now())

    def ack_message(self, message_id: str, version: int) -> None:
        self.store.delete_message(message_id, version)

    def requeue_message(self, message_id: str, version: int) -> None:
        message = self.store.find_message(message_id)
        queue = self.store.find_queue(message.queue)
        if message.delivery_count >= queue.max_delivery_count:
            if queue.dead_letter_queue is not None:
                self.store.move_message_to_dlq(
                    message_id, version, queue.dead_letter_queue, self._now()
                )
            else:
                self.store.delete_message(message_id, version)
        else:
            self.store.requeue_message(message_id, version, self._now())

    def find_message(self, message_id: str) -> Message:
        return self.store.find_message(message_id)

    def _now(self) -> datetime.datetime:
        return datetime.datetime.now(datetime.UTC)
