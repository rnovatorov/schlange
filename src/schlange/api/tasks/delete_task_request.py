import dataclasses


@dataclasses.dataclass
class DeleteTaskRequest:
    id: str
