from typing import Protocol

from .acquire_lease_request import AcquireLeaseRequest
from .acquire_lease_response import AcquireLeaseResponse
from .is_holder_lease_request import IsHolderLeaseRequest
from .is_holder_lease_response import IsHolderLeaseResponse
from .refresh_lease_request import RefreshLeaseRequest
from .refresh_lease_response import RefreshLeaseResponse
from .release_lease_request import ReleaseLeaseRequest


class LeaseServer(Protocol):
    """
    Etcd-compatible leases API, gRPC-style: each method takes a
    single request dataclass and returns a single response dataclass
    (`release` is fire-and-forget). `refresh` re-arms `expires_at` using
    the ttl granted at acquire time (etcd keepalive semantics), so it
    needs no ttl argument.
    """

    def acquire(self, request: AcquireLeaseRequest) -> AcquireLeaseResponse: ...

    def refresh(self, request: RefreshLeaseRequest) -> RefreshLeaseResponse: ...

    def release(self, request: ReleaseLeaseRequest) -> None: ...

    def is_holder(self, request: IsHolderLeaseRequest) -> IsHolderLeaseResponse: ...
