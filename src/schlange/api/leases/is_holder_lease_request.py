import dataclasses


@dataclasses.dataclass
class IsHolderLeaseRequest:
    key: str
    holder: str
