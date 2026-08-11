import typing

from schlange.api import tasks
from schlange.services.execution import core


class TaskServiceAdapter:
    """Adapts tasks API to execution core TaskServicePort."""

    def __init__(self, task_server: tasks.Server) -> None:
        self.task_server = task_server

    def end_execution(
        self, task_id: str, seq_num: int, error: typing.Optional[str]
    ) -> None:
        try:
            self.task_server.end_execution(
                tasks.EndExecutionRequest(
                    task_id=task_id,
                    seq_num=seq_num,
                    error=error,
                )
            )
        except tasks.ConflictError:
            raise core.AbortedError() from None
        except tasks.NotFoundError:
            raise core.NotFoundError() from None
        except tasks.FailedPreconditionError:
            raise core.FailedPreconditionError() from None
