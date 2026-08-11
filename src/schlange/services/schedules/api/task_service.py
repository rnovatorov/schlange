from schlange.api import tasks
from schlange.internal import core


class TaskServiceAdapter:
    """Adapts tasks API to schedules core TaskService.

    create_task is idempotent for a given id: a duplicate (a schedule
    iteration that already fired) is swallowed — the deterministic id is
    the schedules-side idempotency key, so a duplicate means the task
    already exists and the firing is a no-op replay.
    """

    def __init__(self, task_server: tasks.Server) -> None:
        self.task_server = task_server

    def create_task(
        self,
        id: str,
        args: core.DTO,
        kind: str,
        delay: float,
        visibility_timeout: float,
        retry_policy: core.RetryPolicy,
        schedule_id: str,
    ) -> None:
        try:
            self.task_server.create_task(
                tasks.CreateTaskRequest(
                    id=id,
                    args=args,
                    kind=kind,
                    delay=delay,
                    visibility_timeout=visibility_timeout,
                    retry_policy=tasks.RetryPolicy(
                        initial_delay=retry_policy.initial_delay,
                        backoff_factor=retry_policy.backoff_factor,
                        max_delay=retry_policy.max_delay,
                        max_attempts=retry_policy.max_attempts,
                    ),
                    schedule_id=schedule_id,
                )
            )
        except tasks.AlreadyExistsError:
            pass
