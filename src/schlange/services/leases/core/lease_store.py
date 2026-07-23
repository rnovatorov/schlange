import datetime
from typing import Optional, Protocol


class LeaseStore(Protocol):
    """
    Lease persistence interface. `now` is passed in so all time logic
    lives in the service. acquire/refresh return the new expires_at
    (None on failure).
    """

    def acquire(
        self, key: str, holder: str, now: datetime.datetime, ttl: float
    ) -> Optional[datetime.datetime]: ...

    def refresh(
        self, key: str, holder: str, now: datetime.datetime
    ) -> Optional[datetime.datetime]: ...

    def release(self, key: str, holder: str) -> None: ...

    def is_holder(self, key: str, holder: str, now: datetime.datetime) -> bool: ...

    def delete_expired(self, now: datetime.datetime) -> int: ...
