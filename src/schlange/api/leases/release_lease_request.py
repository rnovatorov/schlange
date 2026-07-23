import dataclasses


@dataclasses.dataclass
class ReleaseLeaseRequest:
    key: str
    holder: str
