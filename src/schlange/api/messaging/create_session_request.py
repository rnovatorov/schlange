import dataclasses


@dataclasses.dataclass
class CreateSessionRequest:
    queue: str
    dead_letter: bool = False
