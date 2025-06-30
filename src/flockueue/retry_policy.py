import dataclasses
from typing import Optional

from .errors import TooManyAttempts


@dataclasses.dataclass
class RetryPolicy:

    initial_delay: float
    backoff_factor: float
    max_delay: Optional[float]
    max_attempts: Optional[int]

    def delay(self, attempts: int) -> float:
        if attempts == 0:
            return self.initial_delay
        if self.max_attempts is not None and attempts >= self.max_attempts:
            raise TooManyAttempts()
        delay = self.delay(attempts - 1) * self.backoff_factor
        return delay if self.max_delay is None else min(delay, self.max_delay)

    def total_delay(self) -> float:
        if self.max_attempts is None:
            return float("inf")
        return sum(self.delay(i) for i in range(self.max_attempts))


DEFAULT_RETRY_POLICY = RetryPolicy(
    initial_delay=1,
    backoff_factor=2.0,
    max_delay=24 * 60 * 60,
    max_attempts=20,
)
