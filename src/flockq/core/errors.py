class Error(Exception):
    pass


class TooManyAttemptsError(Error):
    pass


class TaskNotFoundError(Error):
    pass


class TaskNotActiveError(Error):
    pass


class TaskNotReadyError(Error):
    pass


class TaskHandlerNotFound(Error):
    pass


class TaskAlreadyExistsError(Error):
    pass


class TaskUpdatedConcurrentlyError(Error):
    pass
