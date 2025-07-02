class Error(Exception):
    pass


class TooManyAttempts(Error):
    pass


class TaskLocked(Error):
    pass


class TaskNotFound(Error):
    pass


class TaskNotActive(Error):
    pass


class TaskNotReady(Error):
    pass
