import datetime
from typing import Optional

from schlange.internal import sqlite

# Atomic acquire: INSERT if free, else take over the row when it is held
# by the same holder OR has expired. RETURNING yields the new expires_at
# when the row is inserted/updated; no row means a live conflict.
# Timestamps are stored as Unix epoch (REAL) seconds, so expiry is
# computed as now + ttl via plain arithmetic.
SQL_ACQUIRE = """
    INSERT INTO leases (key, holder, ttl, expires_at)
    VALUES (:key, :holder, :ttl, :now + :ttl)
    ON CONFLICT(key) DO UPDATE
    SET holder = :holder,
        ttl = :ttl,
        expires_at = :now + :ttl
    WHERE leases.holder = :holder OR leases.expires_at <= :now
    RETURNING expires_at
"""

# Single-statement refresh: re-arm expires_at = now + stored ttl. The
# ttl column is read in-place, so no separate SELECT is needed.
# RETURNING yields the new expires_at; no row means wrong holder or
# expired.
SQL_REFRESH = """
    UPDATE leases
    SET expires_at = :now + ttl
    WHERE key = :key AND holder = :holder AND expires_at > :now
    RETURNING expires_at
"""

SQL_RELEASE = """
    DELETE FROM leases
    WHERE key = :key AND holder = :holder
"""

SQL_IS_HOLDER = """
    SELECT 1
    FROM leases
    WHERE key = :key AND holder = :holder AND expires_at > :now
"""

SQL_DELETE_EXPIRED = """
    DELETE FROM leases
    WHERE expires_at <= :now
"""


class Store:

    def __init__(self, db: sqlite.Database) -> None:
        self.db = db
        self.data_mapper = sqlite.DataMapper()

    def acquire(
        self, key: str, holder: str, now: datetime.datetime, ttl: float
    ) -> Optional[datetime.datetime]:
        params = {
            "key": key,
            "holder": holder,
            "ttl": ttl,
            "now": self.data_mapper.dump_timestamp(now),
        }
        with self.db.transaction() as tx:
            try:
                row = tx.query_row(SQL_ACQUIRE, params)
            except sqlite.NoRowsError:
                return None
            return self.data_mapper.load_timestamp(row[0])

    def refresh(
        self, key: str, holder: str, now: datetime.datetime
    ) -> Optional[datetime.datetime]:
        params = {
            "key": key,
            "holder": holder,
            "now": self.data_mapper.dump_timestamp(now),
        }
        with self.db.transaction() as tx:
            try:
                row = tx.query_row(SQL_REFRESH, params)
            except sqlite.NoRowsError:
                return None
            return self.data_mapper.load_timestamp(row[0])

    def release(self, key: str, holder: str) -> None:
        with self.db.transaction() as tx:
            tx.execute(SQL_RELEASE, {"key": key, "holder": holder})

    def is_holder(self, key: str, holder: str, now: datetime.datetime) -> bool:
        params = {
            "key": key,
            "holder": holder,
            "now": self.data_mapper.dump_timestamp(now),
        }
        with self.db.transaction(read_only=True) as tx:
            try:
                tx.query_row(SQL_IS_HOLDER, params)
                return True
            except sqlite.NoRowsError:
                return False

    def delete_expired(self, now: datetime.datetime) -> int:
        params = {"now": self.data_mapper.dump_timestamp(now)}
        with self.db.transaction() as tx:
            return tx.execute(SQL_DELETE_EXPIRED, params)
