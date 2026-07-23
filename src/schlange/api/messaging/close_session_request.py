import dataclasses


@dataclasses.dataclass
class CloseSessionRequest:
    session_id: str
