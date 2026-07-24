import enum


class TaskState(enum.Enum):

    ACTIVE = "ACTIVE"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
