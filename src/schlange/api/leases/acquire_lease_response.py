import dataclasses
from typing import Optional

from .lease import Lease


@dataclasses.dataclass
class AcquireLeaseResponse:
    lease: Optional[Lease]
