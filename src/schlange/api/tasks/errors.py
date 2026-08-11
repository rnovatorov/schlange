class Error(Exception):
    pass


class AlreadyExistsError(Error):
    pass


class ConflictError(Error):
    pass


class NotFoundError(Error):
    pass


class FailedPreconditionError(Error):
    pass
