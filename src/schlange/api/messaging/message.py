import dataclasses


@dataclasses.dataclass
class Message:
    id: str
    routing_key: str
    payload: bytes
