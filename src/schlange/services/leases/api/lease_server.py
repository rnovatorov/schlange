import dataclasses

from schlange.api import leases
from schlange.services.leases import core


@dataclasses.dataclass
class LeaseServer:
    """
    Thin public-facing adapter. Wraps a leases core service,
    packs core return values into the gRPC-style response dataclasses.
    """

    service: core.LeaseService

    def acquire(
        self, request: leases.AcquireLeaseRequest
    ) -> leases.AcquireLeaseResponse:
        expires_at = self.service.acquire(request.key, request.holder, request.ttl)
        return leases.AcquireLeaseResponse(
            lease=(
                leases.Lease(
                    key=request.key, holder=request.holder, expires_at=expires_at
                )
                if expires_at is not None
                else None
            )
        )

    def refresh(
        self, request: leases.RefreshLeaseRequest
    ) -> leases.RefreshLeaseResponse:
        expires_at = self.service.refresh(request.key, request.holder)
        return leases.RefreshLeaseResponse(
            lease=(
                leases.Lease(
                    key=request.key, holder=request.holder, expires_at=expires_at
                )
                if expires_at is not None
                else None
            )
        )

    def release(self, request: leases.ReleaseLeaseRequest) -> None:
        self.service.release(request.key, request.holder)

    def is_holder(
        self, request: leases.IsHolderLeaseRequest
    ) -> leases.IsHolderLeaseResponse:
        return leases.IsHolderLeaseResponse(
            is_holder=self.service.is_holder(request.key, request.holder)
        )
