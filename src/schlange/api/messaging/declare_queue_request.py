import dataclasses


@dataclasses.dataclass
class DeclareQueueRequest:
    name: str
    dead_letter_queue: str | None
    max_delivery_count: int
