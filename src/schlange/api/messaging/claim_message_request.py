import dataclasses


@dataclasses.dataclass
class ClaimMessageRequest:
    queue: str
