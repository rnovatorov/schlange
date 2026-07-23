class Error(Exception):
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


class TaskExecutionNotEndedYetError(Error):
    pass


class TaskExecutionNotBegunYetError(Error):
    pass


class TaskNotFailedError(Error):
    pass
