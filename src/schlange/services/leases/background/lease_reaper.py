from schlange.internal import background
from schlange.services.leases import core


class LeaseReaper(background.Worker):
    """Periodically deletes expired leases."""

    def __init__(self, service: core.LeaseService, interval: float) -> None:
        super().__init__(name="LeaseReaper", interval=interval)
        self.service = service

    def work(self) -> None:
        self.service.delete_expired()
