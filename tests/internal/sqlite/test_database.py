import contextlib
import pathlib
import tempfile
import threading
import unittest

from schlange.internal import sqlite
from schlange.internal.sqlite import Migration


def _open(path: pathlib.Path):
    return sqlite.Database.open(path=path, read_pool_capacity=1, write_pool_capacity=1)


class DatabaseMigrateTest(unittest.TestCase):

    def test_migrate_applies_pending_migration(self):
        migration = Migration(
            statements=["CREATE TABLE things (id INTEGER PRIMARY KEY)"],
        )
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "db.sqlite"
            with _open(path) as db:
                db.migrate([migration])
                with db.transaction() as tx:
                    version = tx.query_row("SELECT version FROM schema_version")[0]
                    name = tx.query_row(
                        "SELECT name FROM sqlite_master WHERE name = 'things'"
                    )[0]
            self.assertEqual(version, 1)
            self.assertEqual(name, "things")

    def test_migrate_skips_already_applied_migration(self):
        # The INSERT is deliberately non-idempotent: re-running the body
        # would raise a UNIQUE constraint. A clean second migrate proves the
        # body was skipped under the lock rather than re-applied.
        migration = Migration(
            statements=[
                "CREATE TABLE marker (k INTEGER PRIMARY KEY)",
                "INSERT INTO marker VALUES (1)",
            ],
        )
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "db.sqlite"
            with _open(path) as db:
                db.migrate([migration])
                db.migrate([migration])
                with db.transaction() as tx:
                    ks = [row[0] for row in tx.query("SELECT k FROM marker")]
            self.assertEqual(ks, [1])

    def test_concurrent_migrate_runs_body_exactly_once(self):
        # N databases racing on the same file. The write lock serializes
        # them; the per-migration version re-check under that lock turns
        # the losers into no-ops. If the body ever re-ran, the INSERT would
        # raise UNIQUE constraint and surface as a thread error.
        #
        # The databases are opened up front so the threads race only on
        # migrate -- not on the first-open PRAGMA journal_mode = WAL, which
        # is set once per file and does not honor busy_timeout while it is
        # being changed.
        migration = Migration(
            statements=[
                "CREATE TABLE marker (k INTEGER PRIMARY KEY)",
                "INSERT INTO marker VALUES (1)",
            ],
        )
        n = 4
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "db.sqlite"
            with contextlib.ExitStack() as stack:
                dbs = [stack.enter_context(_open(path)) for _ in range(n)]
                barrier = threading.Barrier(n)
                errors: list[BaseException] = []

                def run(db) -> None:
                    try:
                        barrier.wait()
                        db.migrate([migration])
                    except BaseException as exc:
                        errors.append(exc)

                threads = [
                    threading.Thread(target=run, args=(dbs[i],)) for i in range(n)
                ]
                for t in threads:
                    t.start()
                for t in threads:
                    t.join()

                self.assertEqual(errors, [])
                with dbs[0].transaction() as tx:
                    version = tx.query_row("SELECT version FROM schema_version")[0]
                    ks = [row[0] for row in tx.query("SELECT k FROM marker")]
            self.assertEqual(version, 1)
            self.assertEqual(ks, [1])


if __name__ == "__main__":
    unittest.main()
