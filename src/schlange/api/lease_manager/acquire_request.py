import dataclasses


@dataclasses.dataclass
class AcquireRequest:
    key: str
    holder: str
    ttl: float
