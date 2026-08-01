class Error(Exception):
    """Base error for messaging domain."""


class QueueNotFoundError(Error):
    pass


class QueueAlreadyExistsError(Error):
    pass


class MessageNotFoundError(Error):
    pass


class NoMessagesAvailable(Error):
    pass
