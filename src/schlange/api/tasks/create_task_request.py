import dataclasses
from typing import Optional

from .retry_policy import RetryPolicy


@dataclasses.dataclass
class CreateTaskRequest:
    kind: str
    args: dict
    delay: float
    retry_policy: RetryPolicy
    id: Optional[str] = None
    schedule_id: Optional[str] = None
