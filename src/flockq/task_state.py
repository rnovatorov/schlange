import enum


class TaskState(enum.StrEnum):

    ACTIVE = enum.auto()
    SUCCEEDED = enum.auto()
    FAILED = enum.auto()
