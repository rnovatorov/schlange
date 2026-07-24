from schlange.api import leases as leases_api


class LeaseService:
    """Adapts leases API to tasks core LeaseService port."""

    def __init__(self, lease_server: leases_api.Server) -> None:
        self.lease_server = lease_server

    def acquire_lease(self, key: str, holder: str, ttl: float) -> bool:
        response = self.lease_server.acquire(
            leases_api.AcquireLeaseRequest(
                key=key,
                holder=holder,
                ttl=ttl,
            )
        )
        return response.lease is not None
