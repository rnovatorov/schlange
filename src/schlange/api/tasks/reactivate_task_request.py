import dataclasses


@dataclasses.dataclass
class ReactivateTaskRequest:
    id: str
    delay: float
