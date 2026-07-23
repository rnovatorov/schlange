from schlange.internal import background
from schlange.services.messaging import core


class MessagingSweeper(background.Worker):
    """Periodically sweeps stale consumer sessions."""

    def __init__(self, service: core.MessagingService, interval: float) -> None:
        super().__init__(name="MessagingSweeper", interval=interval)
        self.service = service

    def work(self) -> None:
        self.service.sweep()
