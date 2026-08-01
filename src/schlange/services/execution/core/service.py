import dataclasses

from .errors import NotFoundError
from .handler import Handler, TaskExecution
from .task_service import TaskServicePort


@dataclasses.dataclass
class ExecutionService:
    """Executes task handlers and records the result via the task service port."""

    handlers: dict[str, Handler]
    task_service: TaskServicePort

    def execute(
        self,
        task_id: str,
        seq_num: int,
        kind: str,
        args: dict,
    ) -> None:
        """Execute a task handler and record the result.

        Looks up the handler by kind, runs it, then calls end_execution.
        Handler exceptions are caught and recorded as the execution error.
        end_execution exceptions propagate to the caller.

        Raises:
            NotFoundError: No handler registered for the kind.
            AbortedError: Concurrent modification (from end_execution).
            FailedPreconditionError: Task in wrong state (from end_execution).
        """
        handler = self.handlers.get(kind)
        if handler is None:
            raise NotFoundError(f"no handler registered for kind: {kind}")
        execution = TaskExecution(task_id=task_id, seq_num=seq_num, args=args)
        error: str | None = None
        try:
            handler(execution)
        except Exception as exc:
            error = str(exc)
        self.task_service.end_execution(task_id, seq_num, error)
