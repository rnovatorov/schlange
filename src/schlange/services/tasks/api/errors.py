class Error(Exception):
    pass


class ConflictError(Error):
    pass


class NotFoundError(Error):
    pass


class FailedPreconditionError(Error):
    pass
