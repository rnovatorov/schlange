import dataclasses
from typing import Optional


@dataclasses.dataclass
class EndExecutionRequest:
    task_id: str
    seq_num: int
    error: Optional[str] = None
