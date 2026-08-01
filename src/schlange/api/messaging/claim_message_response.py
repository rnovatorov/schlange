import dataclasses

from .message import Message


@dataclasses.dataclass
class ClaimMessageResponse:
    message: Message
