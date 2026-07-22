import dataclasses
from typing import Optional

from .lease import Lease


@dataclasses.dataclass
class RefreshResponse:
    lease: Optional[Lease]
