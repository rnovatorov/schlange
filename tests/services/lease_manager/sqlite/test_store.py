import datetime
import pathlib
import tempfile
import unittest

from schlange.internal import sqlite
from schlange.services.lease_manager import sqlite as lease_sqlite


class StoreTest(unittest.TestCase):

    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        db_path = pathlib.Path(self.dir.name) / "leases.db"
        self.db_ctx = sqlite.Database.open(db_path, read_pool_capacity=4)
        self.db = self.db_ctx.__enter__()
        self.db.migrate(migrations_path=lease_sqlite.MIGRATIONS_PATH)
        self.store = lease_sqlite.Store(self.db)

    def tearDown(self):
        self.db_ctx.__exit__(None, None, None)
        self.dir.cleanup()

    def _now(self) -> datetime.datetime:
        return datetime.datetime.now(datetime.UTC)

    def test_acquire_succeeds_when_key_is_free(self):
        expires_at = self.store.acquire("k", "a", self._now(), 10)
        self.assertIsNotNone(expires_at)

    def test_acquire_fails_when_held_by_another_within_ttl(self):
        now = self._now()
        self.store.acquire("k", "a", now, 10)
        self.assertIsNone(self.store.acquire("k", "b", now, 10))

    def test_acquire_succeeds_when_existing_lease_expired(self):
        now = self._now()
        self.store.acquire("k", "a", now, 1)
        later = now + datetime.timedelta(seconds=2)
        self.assertIsNotNone(self.store.acquire("k", "b", later, 10))

    def test_acquire_by_same_holder_reacquires(self):
        now = self._now()
        self.store.acquire("k", "a", now, 10)
        self.assertIsNotNone(self.store.acquire("k", "a", now, 10))

    def test_refresh_succeeds_while_held_and_not_expired(self):
        now = self._now()
        self.store.acquire("k", "a", now, 10)
        self.assertIsNotNone(self.store.refresh("k", "a", now))

    def test_refresh_fails_after_ttl_elapsed(self):
        now = self._now()
        self.store.acquire("k", "a", now, 1)
        later = now + datetime.timedelta(seconds=2)
        self.assertIsNone(self.store.refresh("k", "a", later))

    def test_refresh_fails_when_not_holder(self):
        now = self._now()
        self.store.acquire("k", "a", now, 10)
        self.assertIsNone(self.store.refresh("k", "b", now))

    def test_release_lets_another_holder_acquire(self):
        now = self._now()
        self.store.acquire("k", "a", now, 10)
        self.store.release("k", "a")
        self.assertIsNotNone(self.store.acquire("k", "b", now, 10))

    def test_release_is_noop_when_not_holder(self):
        now = self._now()
        self.store.acquire("k", "a", now, 10)
        self.store.release("k", "b")
        self.assertTrue(self.store.is_holder("k", "a", now))

    def test_is_holder_true_when_held_and_not_expired(self):
        now = self._now()
        self.store.acquire("k", "a", now, 10)
        self.assertTrue(self.store.is_holder("k", "a", now))

    def test_is_holder_false_when_expired(self):
        now = self._now()
        self.store.acquire("k", "a", now, 1)
        later = now + datetime.timedelta(seconds=2)
        self.assertFalse(self.store.is_holder("k", "a", later))

    def test_is_holder_false_when_held_by_another(self):
        now = self._now()
        self.store.acquire("k", "a", now, 10)
        self.assertFalse(self.store.is_holder("k", "b", now))

    def test_is_holder_false_when_key_unknown(self):
        self.assertFalse(self.store.is_holder("k", "a", self._now()))

    def test_delete_expired_removes_expired_leases(self):
        now = self._now()
        self.store.acquire("expired", "a", now, 1)
        self.store.acquire("live", "a", now, 10)
        later = now + datetime.timedelta(seconds=2)
        deleted = self.store.delete_expired(later)
        self.assertEqual(deleted, 1)
        self.assertFalse(self.store.is_holder("expired", "a", later))
        self.assertTrue(self.store.is_holder("live", "a", later))

    def test_delete_expired_returns_zero_when_nothing_expired(self):
        now = self._now()
        self.store.acquire("k", "a", now, 10)
        self.assertEqual(self.store.delete_expired(now), 0)

    def test_delete_expired_returns_zero_when_empty(self):
        self.assertEqual(self.store.delete_expired(self._now()), 0)


if __name__ == "__main__":
    unittest.main()
