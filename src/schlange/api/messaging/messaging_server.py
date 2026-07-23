from typing import Protocol

from .ack_message_request import AckMessageRequest
from .claim_message_request import ClaimMessageRequest
from .claim_message_response import ClaimMessageResponse
from .close_session_request import CloseSessionRequest
from .create_session_request import CreateSessionRequest
from .create_session_response import CreateSessionResponse
from .heartbeat_session_request import HeartbeatSessionRequest
from .nack_message_request import NackMessageRequest
from .publish_message_request import PublishMessageRequest
from .publish_message_response import PublishMessageResponse


class MessagingServer(Protocol):
    """
    Public messaging API, gRPC-style: each method takes a single
    request dataclass and returns a single response dataclass
    (`ack`, `nack`, `heartbeat` and `close_session` are
    fire-and-forget).
    """

    def publish(self, request: PublishMessageRequest) -> PublishMessageResponse: ...

    def claim(self, request: ClaimMessageRequest) -> ClaimMessageResponse: ...

    def ack(self, request: AckMessageRequest) -> None: ...

    def nack(self, request: NackMessageRequest) -> None: ...

    def create_session(
        self, request: CreateSessionRequest
    ) -> CreateSessionResponse: ...

    def heartbeat(self, request: HeartbeatSessionRequest) -> None: ...

    def close_session(self, request: CloseSessionRequest) -> None: ...
