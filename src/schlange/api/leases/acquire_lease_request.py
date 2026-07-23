import dataclasses


@dataclasses.dataclass
class AcquireLeaseRequest:
    key: str
    holder: str
    ttl: float
