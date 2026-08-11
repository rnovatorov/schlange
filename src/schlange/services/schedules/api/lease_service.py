from schlange.api import leases


class LeaseService:
    """Adapts leases API to schedules core LeaseService."""

    def __init__(self, lease_server: leases.Server) -> None:
        self.lease_server = lease_server

    def acquire_lease(self, key: str, holder: str, ttl: float) -> bool:
        response = self.lease_server.acquire(
            leases.AcquireLeaseRequest(
                key=key,
                holder=holder,
                ttl=ttl,
            )
        )
        return response.lease is not None
