class Error(Exception):
    pass


class ScheduleUpdatedConcurrentlyError(Error):
    pass


class ScheduleNotEnabledError(Error):
    pass


class ScheduleNotReadyError(Error):
    pass


class ScheduleFiringNotEndedYetError(Error):
    pass


class ScheduleFiringNotBegunYetError(Error):
    pass


class ScheduleAlreadyExistsError(Error):
    pass


class ScheduleNotFoundError(Error):
    pass
