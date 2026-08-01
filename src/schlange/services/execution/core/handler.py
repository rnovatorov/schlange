import dataclasses
import typing


@dataclasses.dataclass
class TaskExecution:
    """Data passed to a handler when executing a task."""

    task_id: str
    seq_num: int
    args: dict


Handler = typing.Callable[[TaskExecution], None]
