import dataclasses


@dataclasses.dataclass
class RefreshRequest:
    key: str
    holder: str
