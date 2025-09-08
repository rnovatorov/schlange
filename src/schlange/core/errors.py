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


class ScheduleUpdatedConcurrentlyError(Error):
    pass


class ScheduleNotEnabledError(Error):
    pass


class ScheduleNotReadyError(Error):
    pass


class ScheduleFiringNotEndedYetError(Error):
    pass


class ScheduleFiringAlreadyEndedError(Error):
    pass


class ScheduleFiringNotBegunYetError(Error):
    pass


class ScheduleAlreadyExistsError(Error):
    pass


class ScheduleNotFoundError(Error):
    pass
