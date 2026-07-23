import dataclasses
from typing import Optional

from .message import Message


@dataclasses.dataclass
class ClaimMessageResponse:
    message: Optional[Message]
