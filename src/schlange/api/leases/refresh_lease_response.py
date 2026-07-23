import dataclasses
from typing import Optional

from .lease import Lease


@dataclasses.dataclass
class RefreshLeaseResponse:
    lease: Optional[Lease]
