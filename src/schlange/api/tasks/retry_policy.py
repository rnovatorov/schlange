import dataclasses
from typing import Optional


@dataclasses.dataclass
class RetryPolicy:
    initial_delay: float
    backoff_factor: float
    max_delay: Optional[float]
    max_attempts: int
