import dataclasses
import datetime
from typing import Optional

from .lease_store import LeaseStore


@dataclasses.dataclass
class LeaseService:
    """
    Stateless lease business logic. Owns the clock; delegates
    persistence to a LeaseStore.
    """

    store: LeaseStore

    def acquire(self, key: str, holder: str, ttl: float) -> Optional[datetime.datetime]:
        return self.store.acquire(key, holder, self._now(), ttl)

    def refresh(self, key: str, holder: str) -> Optional[datetime.datetime]:
        return self.store.refresh(key, holder, self._now())

    def release(self, key: str, holder: str) -> None:
        self.store.release(key, holder)

    def is_holder(self, key: str, holder: str) -> bool:
        return self.store.is_holder(key, holder, self._now())

    def delete_expired(self) -> int:
        return self.store.delete_expired(self._now())

    def _now(self) -> datetime.datetime:
        return datetime.datetime.now(datetime.UTC)
