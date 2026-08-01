import dataclasses


@dataclasses.dataclass
class Message:
    id: str
    queue: str
    payload: bytes
    visibility_timeout: float
    delivery_count: int
    version: int
