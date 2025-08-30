import abc
from typing import Iterable, List

from .event import Event


class Aggregate(abc.ABC):

    def __init__(self, id: str) -> None:
        self._id = id
        self._change_log: List[Event] = []

    @classmethod
    def rehydrate(cls, id: str, events: Iterable[Event]):
        agg = cls(id=id)
        for event in events:
            agg._apply(event)
        return agg

    @property
    def id(self) -> str:
        return self._id

    @property
    def change_log(self) -> List[Event]:
        return self._change_log

    @abc.abstractmethod
    def _apply(self, event: Event) -> None:
        pass

    def _emit(self, event: Event) -> None:
        self._apply(event)
        self.change_log.append(event)
