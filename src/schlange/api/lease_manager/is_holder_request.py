import dataclasses


@dataclasses.dataclass
class IsHolderRequest:
    key: str
    holder: str
