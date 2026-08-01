class Error(Exception):
    """Base error for execution service operations."""


class AbortedError(Error):
    """Concurrent modification conflict. Retriable."""


class NotFoundError(Error):
    """Resource not found. Permanent."""


class FailedPreconditionError(Error):
    """System not in the expected state. Permanent."""
