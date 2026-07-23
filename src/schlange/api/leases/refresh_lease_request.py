import dataclasses


@dataclasses.dataclass
class RefreshLeaseRequest:
    key: str
    holder: str
