from schlange.internal import background
from schlange.services.messaging import core


class Sweeper(background.Worker):
    """Periodically sweeps stale consumer sessions."""

    def __init__(self, service: core.Service, interval: float) -> None:
        super().__init__(name="Sweeper", interval=interval)
        self.service = service

    def work(self) -> None:
        self.service.sweep()
