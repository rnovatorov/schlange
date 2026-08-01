from typing import Protocol

from .ack_message_request import AckMessageRequest
from .claim_message_request import ClaimMessageRequest
from .claim_message_response import ClaimMessageResponse
from .declare_queue_request import DeclareQueueRequest
from .nack_message_request import NackMessageRequest
from .publish_message_request import PublishMessageRequest
from .publish_message_response import PublishMessageResponse


class Server(Protocol):
    """
    Public messaging API, gRPC-style: each method takes a single
    request dataclass and returns a single response dataclass
    (``ack_message``, ``nack_message`` and ``declare_queue`` are
    fire-and-forget).
    """

    def declare_queue(self, request: DeclareQueueRequest) -> None: ...

    def publish_message(
        self, request: PublishMessageRequest
    ) -> PublishMessageResponse: ...

    def claim_message(
        self, request: ClaimMessageRequest
    ) -> ClaimMessageResponse: ...

    def ack_message(self, request: AckMessageRequest) -> None: ...

    def nack_message(self, request: NackMessageRequest) -> None: ...
