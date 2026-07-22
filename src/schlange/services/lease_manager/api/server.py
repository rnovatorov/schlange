import dataclasses

from schlange.api import lease_manager
from schlange.services.lease_manager import core


@dataclasses.dataclass
class Server:
    """
    Thin public-facing adapter. Wraps a lease manager core service,
    packs core return values into the gRPC-style response dataclasses.
    """

    service: core.Service

    def acquire(
        self, request: lease_manager.AcquireRequest
    ) -> lease_manager.AcquireResponse:
        expires_at = self.service.acquire(request.key, request.holder, request.ttl)
        return lease_manager.AcquireResponse(
            lease=(
                lease_manager.Lease(
                    key=request.key, holder=request.holder, expires_at=expires_at
                )
                if expires_at is not None
                else None
            )
        )

    def refresh(
        self, request: lease_manager.RefreshRequest
    ) -> lease_manager.RefreshResponse:
        expires_at = self.service.refresh(request.key, request.holder)
        return lease_manager.RefreshResponse(
            lease=(
                lease_manager.Lease(
                    key=request.key, holder=request.holder, expires_at=expires_at
                )
                if expires_at is not None
                else None
            )
        )

    def release(self, request: lease_manager.ReleaseRequest) -> None:
        self.service.release(request.key, request.holder)

    def is_holder(
        self, request: lease_manager.IsHolderRequest
    ) -> lease_manager.IsHolderResponse:
        return lease_manager.IsHolderResponse(
            is_holder=self.service.is_holder(request.key, request.holder)
        )
