from schlange.api import tasks
from schlange.internal import core as internal_core
from schlange.services.tasks import core


class DataMapper:

    def dump_task(self, task: core.Task) -> tasks.Task:
        return tasks.Task(
            id=task.id,
            kind=task.kind,
            args=task.args,
            state=tasks.TaskState(task.state.value),
        )

    def dump_retry_policy(self, policy: internal_core.RetryPolicy) -> tasks.RetryPolicy:
        return tasks.RetryPolicy(
            initial_delay=policy.initial_delay,
            backoff_factor=policy.backoff_factor,
            max_delay=policy.max_delay,
            max_attempts=policy.max_attempts,
        )

    def load_retry_policy(self, policy: tasks.RetryPolicy) -> internal_core.RetryPolicy:
        return internal_core.RetryPolicy(
            initial_delay=policy.initial_delay,
            backoff_factor=policy.backoff_factor,
            max_delay=policy.max_delay,
            max_attempts=policy.max_attempts,
        )
