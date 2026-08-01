import dataclasses


@dataclasses.dataclass
class PublishMessageRequest:
    queue: str
    payload: bytes
