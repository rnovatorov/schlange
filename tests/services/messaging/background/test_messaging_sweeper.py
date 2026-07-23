import datetime
import pathlib
import tempfile
import unittest
import uuid

from schlange.api import leases
from schlange.internal import sqlite
from schlange.services.messaging import core
from schlange.services.messaging import sqlite as messaging_sqlite
from schlange.services.messaging.background import MessagingSweeper


class AlwaysLeaderLeaseServer:
    """Fake LeaseServer that always grants the lease."""

    def acquire(
        self, request: leases.AcquireLeaseRequest
    ) -> leases.AcquireLeaseResponse:
        return leases.AcquireLeaseResponse(
            lease=leases.Lease(
                key=request.key,
                holder=request.holder,
                expires_at=datetime.datetime.now(datetime.UTC),
            )
        )

    def refresh(
        self, request: leases.RefreshLeaseRequest
    ) -> leases.RefreshLeaseResponse:
        raise NotImplementedError

    def release(self, request: leases.ReleaseLeaseRequest) -> None:
        raise NotImplementedError

    def is_holder(
        self, request: leases.IsHolderLeaseRequest
    ) -> leases.IsHolderLeaseResponse:
        raise NotImplementedError


class NeverLeaderLeaseServer:
    """Fake LeaseServer that always denies the lease."""

    def acquire(
        self, request: leases.AcquireLeaseRequest
    ) -> leases.AcquireLeaseResponse:
        return leases.AcquireLeaseResponse(lease=None)

    def refresh(
        self, request: leases.RefreshLeaseRequest
    ) -> leases.RefreshLeaseResponse:
        raise NotImplementedError

    def release(self, request: leases.ReleaseLeaseRequest) -> None:
        raise NotImplementedError

    def is_holder(
        self, request: leases.IsHolderLeaseRequest
    ) -> leases.IsHolderLeaseResponse:
        raise NotImplementedError


class SweeperTest(unittest.TestCase):

    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        db_path = pathlib.Path(self.dir.name) / "messaging.db"
        self.db_ctx = sqlite.Database.open(db_path, read_pool_capacity=4)
        self.db = self.db_ctx.__enter__()
        self.db.migrate(migrations_path=messaging_sqlite.MIGRATIONS_PATH)
        self.store = messaging_sqlite.MessagingStore(self.db)

    def tearDown(self):
        self.db_ctx.__exit__(None, None, None)
        self.dir.cleanup()

    def _now(self) -> datetime.datetime:
        return datetime.datetime.now(datetime.UTC)

    def _make_service(self, lease_server: leases.LeaseServer) -> core.MessagingService:
        return core.MessagingService(
            store=self.store,
            lease_server=lease_server,
            holder_id="sweeper-1",
            session_timeout=5.0,
        )

    def test_sweep_as_leader(self):
        service = self._make_service(AlwaysLeaderLeaseServer())
        sweeper = MessagingSweeper(service=service, interval=1.0)
        stale_at = self._now() - datetime.timedelta(seconds=10)
        stale_id = str(uuid.uuid4())
        self.store.create_session(stale_id, "orders", False, stale_at)
        self.assertIn(
            stale_id,
            self.store.find_stale_sessions(self._now() - datetime.timedelta(seconds=5)),
        )
        sweeper.work()
        self.assertEqual(self.store.find_stale_sessions(self._now()), [])

    def test_sweep_not_leader(self):
        service = self._make_service(NeverLeaderLeaseServer())
        sweeper = MessagingSweeper(service=service, interval=1.0)
        stale_at = self._now() - datetime.timedelta(seconds=10)
        stale_id = str(uuid.uuid4())
        self.store.create_session(stale_id, "orders", False, stale_at)
        sweeper.work()
        self.assertIn(
            stale_id,
            self.store.find_stale_sessions(self._now() - datetime.timedelta(seconds=5)),
        )


if __name__ == "__main__":
    unittest.main()
