import dataclasses


@dataclasses.dataclass
class Message:
    id: str
    queue: str
    payload: bytes
    version: int
