from typing import Protocol

from .create_task_request import CreateTaskRequest
from .create_task_response import CreateTaskResponse
from .delete_task_request import DeleteTaskRequest
from .end_execution_request import EndExecutionRequest
from .get_task_request import GetTaskRequest
from .get_task_response import GetTaskResponse
from .list_tasks_request import ListTasksRequest
from .list_tasks_response import ListTasksResponse
from .reactivate_task_request import ReactivateTaskRequest
from .reactivate_task_response import ReactivateTaskResponse


class Server(Protocol):
    """
    Public tasks API, gRPC-style: each method takes a single
    request dataclass and returns a single response dataclass
    (`delete_task` and `end_execution` are void-return).
    """

    def create_task(self, request: CreateTaskRequest) -> CreateTaskResponse: ...

    def get_task(self, request: GetTaskRequest) -> GetTaskResponse: ...

    def list_tasks(self, request: ListTasksRequest) -> ListTasksResponse: ...

    def delete_task(self, request: DeleteTaskRequest) -> None: ...

    def reactivate_task(
        self, request: ReactivateTaskRequest
    ) -> ReactivateTaskResponse: ...

    def end_execution(self, request: EndExecutionRequest) -> None: ...
