import dataclasses

from schlange.api import tasks
from schlange.services.tasks import core

from .data_mapper import DataMapper
from .errors import ConflictError, FailedPreconditionError, NotFoundError


@dataclasses.dataclass
class Server:
    """
    Thin public-facing adapter. Wraps a tasks core service,
    packs core return values into the gRPC-style response dataclasses.
    """

    service: core.TaskService
    data_mapper: DataMapper = dataclasses.field(default_factory=DataMapper)

    def create_task(self, request: tasks.CreateTaskRequest) -> tasks.CreateTaskResponse:
        task = self.service.create_task(
            args=request.args,
            kind=request.kind,
            delay=request.delay,
            visibility_timeout=request.visibility_timeout,
            retry_policy=self.data_mapper.load_retry_policy(request.retry_policy),
            id=request.id,
            schedule_id=request.schedule_id,
        )
        return tasks.CreateTaskResponse(task=self.data_mapper.dump_task(task))

    def get_task(self, request: tasks.GetTaskRequest) -> tasks.GetTaskResponse:
        try:
            task = self.service.task(request.id)
        except core.TaskNotFoundError:
            raise NotFoundError() from None
        return tasks.GetTaskResponse(task=self.data_mapper.dump_task(task))

    def list_tasks(self, request: tasks.ListTasksRequest) -> tasks.ListTasksResponse:
        spec = core.TaskSpecification(
            state=(
                core.TaskState(request.state.value)
                if request.state is not None
                else None
            ),
            ready_as_of=request.ready_as_of,
            last_execution_ended_before=request.last_execution_ended_before,
        )
        result = self.service.list_tasks(spec)
        return tasks.ListTasksResponse(
            tasks=[self.data_mapper.dump_task(task) for task in result]
        )

    def delete_task(self, request: tasks.DeleteTaskRequest) -> None:
        try:
            self.service.delete_task(request.id)
        except core.TaskNotFoundError:
            raise NotFoundError() from None

    def reactivate_task(
        self, request: tasks.ReactivateTaskRequest
    ) -> tasks.ReactivateTaskResponse:
        try:
            task = self.service.reactivate_task(request.id, request.delay)
        except core.TaskNotFoundError:
            raise NotFoundError() from None
        except core.TaskNotFailedError:
            raise FailedPreconditionError() from None
        return tasks.ReactivateTaskResponse(task=self.data_mapper.dump_task(task))

    def end_execution(self, request: tasks.EndExecutionRequest) -> None:
        try:
            self.service.end_execution(
                task_id=request.task_id,
                seq_num=request.seq_num,
                error=request.error,
            )
        except core.TaskUpdatedConcurrentlyError:
            raise ConflictError() from None
        except core.TaskNotFoundError:
            raise NotFoundError() from None
        except core.TaskExecutionNotFoundError:
            raise FailedPreconditionError() from None
