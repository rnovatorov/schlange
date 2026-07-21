import datetime

from schlange.internal import core


class DataMapper:

    def dump_timestamp(self, timestamp: datetime.datetime) -> str:
        return timestamp.isoformat()

    def load_timestamp(self, s: str) -> datetime.datetime:
        return datetime.datetime.fromisoformat(s)

    def dump_retry_policy(self, policy: core.RetryPolicy) -> core.DTO:
        return {
            "initial_delay": policy.initial_delay,
            "backoff_factor": policy.backoff_factor,
            "max_delay": policy.max_delay,
            "max_attempts": policy.max_attempts,
        }

    def load_retry_policy(self, dto: core.DTO) -> core.RetryPolicy:
        return core.RetryPolicy(
            initial_delay=dto["initial_delay"],
            backoff_factor=dto["backoff_factor"],
            max_delay=dto.get("max_delay"),
            max_attempts=dto["max_attempts"],
        )
