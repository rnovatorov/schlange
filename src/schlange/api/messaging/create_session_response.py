import dataclasses


@dataclasses.dataclass
class CreateSessionResponse:
    session_id: str
