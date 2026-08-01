import typing


class TaskServicePort(typing.Protocol):
    """Driven port for task lifecycle operations."""

    def end_execution(
        self, task_id: str, seq_num: int, error: typing.Optional[str]
    ) -> None: ...
