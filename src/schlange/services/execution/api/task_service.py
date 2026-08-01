import typing

from schlange.api import tasks as tasks_api
from schlange.services.execution import core
from schlange.services.tasks.api.errors import (
    ConflictError,
    FailedPreconditionError,
    NotFoundError,
)


class TaskServiceAdapter:
    """Adapts tasks API to execution core TaskServicePort."""

    def __init__(self, task_server: tasks_api.Server) -> None:
        self.task_server = task_server

    def end_execution(
        self, task_id: str, seq_num: int, error: typing.Optional[str]
    ) -> None:
        try:
            self.task_server.end_execution(
                tasks_api.EndExecutionRequest(
                    task_id=task_id,
                    seq_num=seq_num,
                    error=error,
                )
            )
        except ConflictError:
            raise core.AbortedError() from None
        except NotFoundError:
            raise core.NotFoundError() from None
        except FailedPreconditionError:
            raise core.FailedPreconditionError() from None
