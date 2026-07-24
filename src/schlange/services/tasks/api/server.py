import dataclasses

from schlange.api import tasks
from schlange.services.tasks import core

from .data_mapper import DataMapper


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
            retry_policy=self.data_mapper.load_retry_policy(request.retry_policy),
            id=request.id,
            schedule_id=request.schedule_id,
        )
        return tasks.CreateTaskResponse(task=self.data_mapper.dump_task(task))

    def get_task(self, request: tasks.GetTaskRequest) -> tasks.GetTaskResponse:
        task = self.service.task(request.id)
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
        self.service.delete_task(request.id)

    def reactivate_task(
        self, request: tasks.ReactivateTaskRequest
    ) -> tasks.ReactivateTaskResponse:
        task = self.service.reactivate_task(request.id, request.delay)
        return tasks.ReactivateTaskResponse(task=self.data_mapper.dump_task(task))

    def end_execution(self, request: tasks.EndExecutionRequest) -> None:
        self.service.end_execution(
            task_id=request.task_id,
            seq_num=request.seq_num,
            error=request.error,
        )
