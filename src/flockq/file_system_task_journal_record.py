import dataclasses
from typing import List

from .event import Event


@dataclasses.dataclass
class FileSystemTaskJournalRecord:

    events: List[Event]
