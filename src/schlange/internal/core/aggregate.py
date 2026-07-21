import dataclasses


@dataclasses.dataclass
class Aggregate:
    """
    Base class for domain aggregates.

    An aggregate is a domain object with identity (id) and an
    optimistic locking version. The version starts at 1, set by the
    create() classmethod of each concrete aggregate.

    Version semantics:
        - The version is incremented on each update.
        - The in-memory aggregate is NOT updated to reflect this.

    After a successful update, the aggregate holds a stale version.
    A subsequent update on the same in-memory object will fail with
    *UpdatedConcurrentlyError.

    This is a known bug. The fix is to have aggregates track both
    the original version (used for optimistic locking) and the new
    version (written on the next save).
    """

    id: str
    version: int
