import abc
from typing import Iterable, List, Optional


class Aggregate[EventType, ProjectionType](abc.ABC):

    def __init__(self, id: str) -> None:
        self._id = id
        self._change_log: List[EventType] = []
        self._projection: Optional[ProjectionType] = None

    @classmethod
    def rehydrate(cls, id: str, events: Iterable[EventType]):
        agg = cls(id=id)
        for event in events:
            agg._apply(event)
        return agg

    @property
    def id(self) -> str:
        return self._id

    @property
    def change_log(self) -> List[EventType]:
        return self._change_log

    @abc.abstractmethod
    def _apply(self, event: EventType) -> None:
        pass

    def _emit(self, event: EventType) -> None:
        self._apply(event)
        self.change_log.append(event)
