import dataclasses


@dataclasses.dataclass
class PublishMessageRequest:
    routing_key: str
    payload: bytes
