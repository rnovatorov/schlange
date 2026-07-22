from typing import Protocol

from .acquire_request import AcquireRequest
from .acquire_response import AcquireResponse
from .is_holder_request import IsHolderRequest
from .is_holder_response import IsHolderResponse
from .refresh_request import RefreshRequest
from .refresh_response import RefreshResponse
from .release_request import ReleaseRequest


class Server(Protocol):
    """
    Etcd-compatible lease manager API, gRPC-style: each method takes a
    single request dataclass and returns a single response dataclass
    (`release` is fire-and-forget). `refresh` re-arms `expires_at` using
    the ttl granted at acquire time (etcd keepalive semantics), so it
    needs no ttl argument.
    """

    def acquire(self, request: AcquireRequest) -> AcquireResponse: ...

    def refresh(self, request: RefreshRequest) -> RefreshResponse: ...

    def release(self, request: ReleaseRequest) -> None: ...

    def is_holder(self, request: IsHolderRequest) -> IsHolderResponse: ...
