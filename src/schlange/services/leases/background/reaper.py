from schlange.internal import background
from schlange.services.leases import core


class Reaper(background.Worker):
    """Periodically deletes expired leases."""

    def __init__(self, service: core.Service, interval: float) -> None:
        super().__init__(name="Reaper", interval=interval)
        self.service = service

    def work(self) -> None:
        self.service.delete_expired()
