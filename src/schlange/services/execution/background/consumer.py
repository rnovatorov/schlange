import json
import logging

from schlange.api import messaging as messaging_api
from schlange.internal import background
from schlange.services.execution import core
from schlange.services.messaging import core as messaging_core

LOGGER = logging.getLogger(__name__)


class Consumer(background.Worker):
    """Claims and processes messages from a single queue.

    Each call of ``work`` claims at most one message, hands it to the
    execution service, then acks or requeues based on the outcome.
    """

    def __init__(
        self,
        queue: str,
        interval: float,
        messaging_server: messaging_api.Server,
        execution_service: core.ExecutionService,
    ) -> None:
        super().__init__(name=f"schlange.Consumer[{queue}]", interval=interval)
        self.queue = queue
        self.messaging_server = messaging_server
        self.execution_service = execution_service

    def work(self) -> None:
        while True:
            try:
                response = self.messaging_server.claim_message(
                    messaging_api.ClaimMessageRequest(queue=self.queue)
                )
            except messaging_core.NoMessagesAvailable:
                return

            message = response.message
            payload = json.loads(message.payload)

            try:
                self.execution_service.execute(
                    task_id=payload["task_id"],
                    seq_num=payload["seq_num"],
                    kind=payload["kind"],
                    args=payload["args"],
                )
            except core.AbortedError:
                LOGGER.debug("requeueing message: id=%s", message.id)
                self.messaging_server.requeue_message(
                    messaging_api.RequeueMessageRequest(
                        message_id=message.id,
                        version=message.version,
                    )
                )
                continue
            except core.Error as exc:
                LOGGER.warning(
                    "permanent error, acking message: id=%s, err=%r",
                    message.id,
                    exc,
                )

            self.messaging_server.ack_message(
                messaging_api.AckMessageRequest(
                    message_id=message.id,
                    version=message.version,
                )
            )
