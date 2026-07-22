import dataclasses


@dataclasses.dataclass
class ReleaseRequest:
    key: str
    holder: str
