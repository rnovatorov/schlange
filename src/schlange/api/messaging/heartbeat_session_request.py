import dataclasses


@dataclasses.dataclass
class HeartbeatSessionRequest:
    session_id: str
