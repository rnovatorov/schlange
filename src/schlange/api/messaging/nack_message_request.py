import dataclasses


@dataclasses.dataclass
class NackMessageRequest:
    message_id: str
    version: int
