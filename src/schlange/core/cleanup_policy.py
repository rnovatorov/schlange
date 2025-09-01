import dataclasses
import datetime


@dataclasses.dataclass
class CleanupPolicy:

    delete_succeeded_after: float
    delete_failed_after: float

    def succeeded_deadline(self, now: datetime.datetime) -> datetime.datetime:
        return now - datetime.timedelta(seconds=self.delete_succeeded_after)

    def failed_deadline(self, now: datetime.datetime) -> datetime.datetime:
        return now - datetime.timedelta(seconds=self.delete_failed_after)
