import dataclasses
from typing import Optional


@dataclasses.dataclass
class EndExecutionRequest:
    task_id: str
    execution_id: str
    error: Optional[str] = None
