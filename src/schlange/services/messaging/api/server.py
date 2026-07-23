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

    def publish(
        self, request: messaging.PublishMessageRequest
    ) -> messaging.PublishMessageResponse:
        message_id = self.service.publish(request.routing_key, request.payload)
        return messaging.PublishMessageResponse(message_id=message_id)

    def claim(
        self, request: messaging.ClaimMessageRequest
    ) -> messaging.ClaimMessageResponse:
        result = self.service.claim(request.session_id)
        if result is None:
            return messaging.ClaimMessageResponse(message=None)
        return messaging.ClaimMessageResponse(
            message=messaging.Message(
                id=result.id,
                routing_key=result.routing_key,
                payload=result.payload,
            )
        )

    def ack(self, request: messaging.AckMessageRequest) -> None:
        self.service.ack(request.message_id)

    def nack(self, request: messaging.NackMessageRequest) -> None:
        self.service.nack(request.message_id)

    def create_session(
        self, request: messaging.CreateSessionRequest
    ) -> messaging.CreateSessionResponse:
        session_id = self.service.create_session(request.queue, request.dead_letter)
        return messaging.CreateSessionResponse(session_id=session_id)

    def heartbeat(self, request: messaging.HeartbeatSessionRequest) -> None:
        self.service.heartbeat(request.session_id)

    def close_session(self, request: messaging.CloseSessionRequest) -> None:
        self.service.close_session(request.session_id)
