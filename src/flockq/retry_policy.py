import dataclasses
from typing import Optional

from .errors import TooManyAttemptsError


@dataclasses.dataclass
class RetryPolicy:

    initial_delay: float
    backoff_factor: float
    max_delay: Optional[float]
    max_attempts: int

    def delay(self, attempts: int) -> float:
        if attempts == 0:
            return 0
        if attempts == 1:
            return self.initial_delay
        if self.max_attempts <= attempts:
            raise TooManyAttemptsError()
        delay = self.delay(attempts - 1) * self.backoff_factor
        return delay if self.max_delay is None else min(delay, self.max_delay)

    def total_delay(self) -> float:
        return sum(self.delay(i) for i in range(self.max_attempts))
