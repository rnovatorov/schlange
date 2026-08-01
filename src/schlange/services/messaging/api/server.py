import dataclasses

from schlange.api import messaging
from schlange.services.messaging import core


@dataclasses.dataclass
class Server:

    service: core.Service

    def declare_queue(self, request: messaging.DeclareQueueRequest) -> None:
        self.service.declare_queue(
            name=request.name,
            dead_letter_queue=request.dead_letter_queue,
            max_delivery_count=request.max_delivery_count,
        )

    def publish_message(
        self, request: messaging.PublishMessageRequest
    ) -> messaging.PublishMessageResponse:
        message_id = self.service.publish_message(
            queue=request.queue,
            payload=request.payload,
            visibility_timeout=request.visibility_timeout,
        )
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
                visibility_timeout=result.visibility_timeout,
                delivery_count=result.delivery_count,
                version=result.version,
            )
        )

    def ack_message(self, request: messaging.AckMessageRequest) -> None:
        self.service.ack_message(request.message_id, request.version)

    def requeue_message(self, request: messaging.RequeueMessageRequest) -> None:
        self.service.requeue_message(request.message_id, request.version)
