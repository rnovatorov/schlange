import datetime

import schlange


class DataMapper:

    def dump_retry_policy(self, policy: schlange.RetryPolicy) -> schlange.DTO:
        return {
            "initial_delay": policy.initial_delay,
            "backoff_factor": policy.backoff_factor,
            "max_delay": policy.max_delay,
            "max_attempts": policy.max_attempts,
        }

    def dump_task(self, task: schlange.Task) -> schlange.DTO:
        return {
            "id": task.id,
            "version": task.version,
            "created_at": self.dump_timestamp(task.created_at),
            "args": task.args,
            "ready_at": self.dump_timestamp(task.ready_at),
            "retry_policy": self.dump_retry_policy(task.retry_policy),
            "execution": [self.dump_task_execution(e) for e in task.executions],
            "schedule_id": task.schedule_id,
        }

    def dump_task_execution(self, execution: schlange.TaskExecution) -> schlange.DTO:
        return {
            "begun_at": self.dump_timestamp(execution.begun_at),
            "ended_at": (
                self.dump_timestamp(execution.ended_at)
                if execution.ended_at is not None
                else None
            ),
            "error": execution.error if execution.error is not None else None,
        }

    def dump_schedule(self, schedule: schlange.Schedule) -> schlange.DTO:
        return {
            "id": schedule.id,
            "version": schedule.version,
            "created_at": self.dump_timestamp(schedule.created_at),
            "ready_at": self.dump_timestamp(schedule.ready_at),
            "origin": self.dump_timestamp(schedule.origin),
            "interval": schedule.interval,
            "retry_policy": self.dump_retry_policy(schedule.retry_policy),
            "enabled": schedule.enabled,
            "task_args": schedule.task_args,
            "task_retry_policy": self.dump_retry_policy(schedule.task_retry_policy),
            "task_sequence_number": schedule.task_sequence_number,
            "firings": [self.dump_schedule_firing(f) for f in schedule.firings],
        }

    def dump_schedule_firing(self, creation: schlange.ScheduleFiring) -> schlange.DTO:
        return {
            "task_sequence_number": creation.task_sequence_number,
            "begun_at": self.dump_timestamp(creation.begun_at),
            "ended_at": (
                self.dump_timestamp(creation.ended_at)
                if creation.ended_at is not None
                else None
            ),
            "error": creation.error if creation.error is not None else None,
        }

    def dump_task_state(self, state: schlange.TaskState) -> str:
        return state.value

    def dump_timestamp(self, timestamp: datetime.datetime) -> str:
        return timestamp.isoformat()
