import dataclasses
import datetime
import typing


@dataclasses.dataclass
class Queue:
    name: str
    dead_letter_queue: typing.Optional[str]
    max_delivery_count: int
    created_at: datetime.datetime
