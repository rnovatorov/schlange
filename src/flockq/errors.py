class Error(Exception):
    pass


class TooManyAttemptsError(Error):
    pass


class TaskLockedError(Error):
    pass


class TaskNotFoundError(Error):
    pass


class TaskNotActiveError(Error):
    pass


class TaskNotReadyError(Error):
    pass
