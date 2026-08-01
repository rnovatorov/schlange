import dataclasses

from schlange.api import messaging
from schlange.services.messaging import core


@dataclasses.dataclass
class Server:
    """
    Thin public-facing adapter. Wraps a messaging core service,
    packs core return values into the gRPC-style response dataclasses.
    """

    service: core.Service

    def declare_queue(self, request: messaging.DeclareQueueRequest) -> None:
        self.service.declare_queue(
            name=request.name,
            dead_letter_queue=request.dead_letter_queue,
            visibility_timeout=request.visibility_timeout,
        )

    def publish_message(
        self, request: messaging.PublishMessageRequest
    ) -> messaging.PublishMessageResponse:
        message_id = self.service.publish_message(request.queue, request.payload)
        return messaging.PublishMessageResponse(message_id=message_id)

    def claim_message(
        self, request: messaging.ClaimMessageRequest
    ) -> messaging.ClaimMessageResponse:
        result = self.service.claim_message(request.queue)
        return messaging.ClaimMessageResponse(
            message=messaging.Message(
                id=result.id,
                queue=result.queue,
                payload=result.payload,
                version=result.version,
            )
        )

    def ack_message(self, request: messaging.AckMessageRequest) -> None:
        self.service.ack_message(request.message_id, request.version)

    def nack_message(self, request: messaging.NackMessageRequest) -> None:
        self.service.nack_message(request.message_id, request.version)
