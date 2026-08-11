import typing

from schlange.internal import core as internal_core


class TaskServicePort(typing.Protocol):
    """Driven port for creating tasks from schedules.

    create_task is idempotent for a given id: a duplicate (a schedule
    iteration that already fired) is a no-op at the seam, so schedule
    firing can use deterministic task ids without handling duplicates.
    """

    def create_task(
        self,
        id: str,
        args: internal_core.DTO,
        kind: str,
        delay: float,
        visibility_timeout: float,
        retry_policy: internal_core.RetryPolicy,
        schedule_id: str,
    ) -> None: ...
