import dataclasses


@dataclasses.dataclass
class Migration:

    statements: list[str]
