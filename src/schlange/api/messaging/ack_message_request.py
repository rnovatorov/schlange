import dataclasses


@dataclasses.dataclass
class AckMessageRequest:
    message_id: str
    version: int
