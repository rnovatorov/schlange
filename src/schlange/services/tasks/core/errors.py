class Error(Exception):
    pass


class TaskNotFoundError(Error):
    pass


class TaskNotActiveError(Error):
    pass


class TaskNotReadyError(Error):
    pass


class TaskAlreadyExistsError(Error):
    pass


class TaskUpdatedConcurrentlyError(Error):
    pass


class TaskNotFailedError(Error):
    pass


class TaskExecutionNotFoundError(Error):
    pass
