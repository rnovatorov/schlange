import dataclasses
from typing import List

from flockq import core


@dataclasses.dataclass
class FileSystemTaskJournalRecord:

    events: List[core.Event]
