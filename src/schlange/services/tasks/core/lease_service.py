from typing import Protocol


class LeaseService(Protocol):
    """Driven port for lease-based leader election."""

    def acquire_lease(self, key: str, holder: str, ttl: float) -> bool:
        """Acquire or renew a lease. Returns True if acquired, False if held by another."""
        ...
