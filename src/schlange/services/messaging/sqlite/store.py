import datetime
from typing import List, Optional

from schlange.internal import sqlite
from schlange.services.messaging import core

SQL_PUBLISH = """
    INSERT INTO messages (id, routing_key, payload, created_at, is_dead_letter)
    VALUES (:id, :routing_key, :payload, :created_at, :is_dead_letter)
"""

# Atomic claim: pick the oldest unclaimed message whose routing_key and
# dead-letter flag match the session's queue and dead-letter flag, and
# assign it to the session. RETURNING yields the claimed message; no
# row means nothing matched or the session does not exist.
SQL_CLAIM = """
    UPDATE messages
    SET claimed_by = :session_id, claimed_at = :now
    WHERE id = (
        SELECT m.id
        FROM messages m
        JOIN sessions s
            ON m.routing_key = s.queue AND m.is_dead_letter = s.dead_letter
        WHERE s.id = :session_id AND m.claimed_by IS NULL
        ORDER BY m.created_at
        LIMIT 1
    )
    RETURNING id, routing_key, payload, created_at, is_dead_letter, claimed_by, claimed_at
"""

SQL_ACK = """
    DELETE FROM messages
    WHERE id = :message_id
"""

# Nack: route to the dead-letter queue (idempotent by nature) and
# release the claim so a dead-letter session can re-claim it.
SQL_NACK = """
    UPDATE messages
    SET is_dead_letter = 1,
        claimed_by = NULL,
        claimed_at = NULL
    WHERE id = :message_id
"""

SQL_CREATE_SESSION = """
    INSERT INTO sessions (id, queue, dead_letter, last_heartbeat_at, created_at)
    VALUES (:id, :queue, :dead_letter, :heartbeat_at, :created_at)
"""

SQL_HEARTBEAT = """
    UPDATE sessions
    SET last_heartbeat_at = :heartbeat_at
    WHERE id = :session_id
"""

SQL_RELEASE_CLAIMS = """
    UPDATE messages
    SET claimed_by = NULL, claimed_at = NULL
    WHERE claimed_by = :session_id
"""

SQL_DELETE_SESSION = """
    DELETE FROM sessions
    WHERE id = :session_id
"""

SQL_FIND_STALE_SESSIONS = """
    SELECT id
    FROM sessions
    WHERE last_heartbeat_at < :threshold
"""

SQL_FIND_MESSAGE = """
    SELECT id, routing_key, payload, created_at, is_dead_letter, claimed_by, claimed_at
    FROM messages
    WHERE id = :id
"""

SQL_FIND_SESSION = """
    SELECT id, queue, dead_letter, last_heartbeat_at, created_at
    FROM sessions
    WHERE id = :id
"""


class Store:

    def __init__(self, db: sqlite.Database) -> None:
        self.db = db
        self.data_mapper = sqlite.DataMapper()

    def publish(
        self,
        message_id: str,
        routing_key: str,
        payload: bytes,
        now: datetime.datetime,
    ) -> None:
        params = {
            "id": message_id,
            "routing_key": routing_key,
            "payload": payload,
            "created_at": self.data_mapper.dump_timestamp(now),
            "is_dead_letter": 0,
        }
        with self.db.transaction() as tx:
            tx.execute(SQL_PUBLISH, params)

    def claim(
        self,
        session_id: str,
        now: datetime.datetime,
    ) -> Optional[core.Message]:
        params = {
            "session_id": session_id,
            "now": self.data_mapper.dump_timestamp(now),
        }
        with self.db.transaction(synchronous=False) as tx:
            try:
                row = tx.query_row(SQL_CLAIM, params)
            except sqlite.NoRowsError:
                return None
            return self._collect_message(row)

    def ack(self, message_id: str) -> None:
        with self.db.transaction(synchronous=False) as tx:
            tx.execute(SQL_ACK, {"message_id": message_id})

    def nack(self, message_id: str) -> None:
        with self.db.transaction(synchronous=False) as tx:
            tx.execute(SQL_NACK, {"message_id": message_id})

    def create_session(
        self,
        session_id: str,
        queue: str,
        dead_letter: bool,
        now: datetime.datetime,
    ) -> None:
        params = {
            "id": session_id,
            "queue": queue,
            "dead_letter": int(dead_letter),
            "heartbeat_at": self.data_mapper.dump_timestamp(now),
            "created_at": self.data_mapper.dump_timestamp(now),
        }
        with self.db.transaction() as tx:
            tx.execute(SQL_CREATE_SESSION, params)

    def heartbeat(self, session_id: str, now: datetime.datetime) -> None:
        params = {
            "session_id": session_id,
            "heartbeat_at": self.data_mapper.dump_timestamp(now),
        }
        with self.db.transaction(synchronous=False) as tx:
            tx.execute(SQL_HEARTBEAT, params)

    def close_session(self, session_id: str) -> None:
        params = {"session_id": session_id}
        with self.db.transaction() as tx:
            tx.execute(SQL_RELEASE_CLAIMS, params)
            tx.execute(SQL_DELETE_SESSION, params)

    def find_stale_sessions(self, threshold: datetime.datetime) -> List[str]:
        params = {"threshold": self.data_mapper.dump_timestamp(threshold)}
        with self.db.transaction(read_only=True) as tx:
            return [row[0] for row in tx.query(SQL_FIND_STALE_SESSIONS, params)]

    def find_message(self, message_id: str) -> Optional[core.Message]:
        with self.db.transaction(read_only=True) as tx:
            try:
                row = tx.query_row(SQL_FIND_MESSAGE, {"id": message_id})
            except sqlite.NoRowsError:
                return None
            return self._collect_message(row)

    def find_session(self, session_id: str) -> Optional[core.Session]:
        with self.db.transaction(read_only=True) as tx:
            try:
                row = tx.query_row(SQL_FIND_SESSION, {"id": session_id})
            except sqlite.NoRowsError:
                return None
            return self._collect_session(row)

    def _collect_message(self, row) -> core.Message:
        return core.Message(
            id=row[0],
            routing_key=row[1],
            payload=row[2],
            created_at=self.data_mapper.load_timestamp(row[3]),
            is_dead_letter=bool(row[4]),
            claimed_by=row[5],
            claimed_at=(
                self.data_mapper.load_timestamp(row[6]) if row[6] is not None else None
            ),
        )

    def _collect_session(self, row) -> core.Session:
        return core.Session(
            id=row[0],
            queue=row[1],
            dead_letter=bool(row[2]),
            last_heartbeat_at=self.data_mapper.load_timestamp(row[3]),
            created_at=self.data_mapper.load_timestamp(row[4]),
        )
