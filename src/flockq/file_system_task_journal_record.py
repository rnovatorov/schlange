import dataclasses
from typing import List

from .task_events import TaskEvent


@dataclasses.dataclass
class FileSystemTaskJournalRecord:

    events: List[TaskEvent]
