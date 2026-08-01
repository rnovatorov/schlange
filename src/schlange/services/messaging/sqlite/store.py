import datetime
import sqlite3

from schlange.internal import sqlite
from schlange.services.messaging import core

SQL_INSERT_QUEUE = """
    INSERT INTO queues (name, dead_letter_queue, visibility_timeout, created_at)
    VALUES (:name, :dead_letter_queue, :visibility_timeout, :created_at)
"""

SQL_FIND_QUEUE = """
    SELECT name, dead_letter_queue, visibility_timeout, created_at
    FROM queues
    WHERE name = :name
"""

SQL_PUBLISH = """
    INSERT INTO messages (id, queue, payload, created_at, visible_at, version)
    VALUES (:id, :queue, :payload, :created_at, :visible_at, :version)
"""

SQL_CLAIM = """
    UPDATE messages
    SET version = version + 1,
        visible_at = :now + (
            SELECT visibility_timeout FROM queues WHERE name = :queue
        )
    WHERE id = (
        SELECT id
        FROM messages
        WHERE queue = :queue AND visible_at <= :now
        ORDER BY created_at
        LIMIT 1
    )
    RETURNING id, queue, payload, created_at, version
"""

SQL_DELETE_MESSAGE = """
    DELETE FROM messages
    WHERE id = :message_id AND version = :version
"""

SQL_MOVE_TO_DLQ = """
    UPDATE messages
    SET queue = :dlq,
        visible_at = :now,
        version = version + 1
    WHERE id = :message_id AND version = :version
"""

SQL_FIND_MESSAGE = """
    SELECT id, queue, payload, created_at, version
    FROM messages
    WHERE id = :id
"""


class Store:

    def __init__(self, db: sqlite.Database) -> None:
        self.db = db
        self.dm = sqlite.DataMapper()

    def declare_queue(
        self,
        name: str,
        dead_letter_queue: str | None,
        visibility_timeout: float,
        now: datetime.datetime,
    ) -> None:
        try:
            with self.db.transaction() as tx:
                tx.execute(
                    SQL_INSERT_QUEUE,
                    {
                        "name": name,
                        "dead_letter_queue": dead_letter_queue,
                        "visibility_timeout": visibility_timeout,
                        "created_at": self.dm.dump_timestamp(now),
                    },
                )
        except sqlite3.IntegrityError as e:
            if e.sqlite_errorname == "SQLITE_CONSTRAINT_PRIMARYKEY":
                raise core.QueueAlreadyExistsError(name) from None
            raise core.QueueNotFoundError(dead_letter_queue) from None

    def find_queue(self, name: str) -> core.Queue:
        with self.db.transaction(read_only=True) as tx:
            try:
                row = tx.query_row(SQL_FIND_QUEUE, {"name": name})
            except sqlite.NoRowsError:
                raise core.QueueNotFoundError(name) from None
            return self._collect_queue(row)

    def publish_message(
        self,
        message_id: str,
        queue: str,
        payload: bytes,
        now: datetime.datetime,
    ) -> None:
        epoch = self.dm.dump_timestamp(now)
        try:
            with self.db.transaction() as tx:
                tx.execute(
                    SQL_PUBLISH,
                    {
                        "id": message_id,
                        "queue": queue,
                        "payload": payload,
                        "created_at": epoch,
                        "visible_at": epoch,
                        "version": 0,
                    },
                )
        except sqlite3.IntegrityError:
            raise core.QueueNotFoundError(queue) from None

    def claim_message(
        self,
        queue: str,
        now: datetime.datetime,
    ) -> core.Message:
        with self.db.transaction(synchronous=False) as tx:
            try:
                row = tx.query_row(
                    SQL_CLAIM,
                    {
                        "queue": queue,
                        "now": self.dm.dump_timestamp(now),
                    },
                )
            except sqlite.NoRowsError:
                raise core.NoMessagesAvailable(queue) from None
            return self._collect_message(row)

    def delete_message(self, message_id: str, version: int) -> None:
        with self.db.transaction(synchronous=False) as tx:
            tx.execute(
                SQL_DELETE_MESSAGE,
                {"message_id": message_id, "version": version},
            )

    def move_message_to_dlq(
        self,
        message_id: str,
        version: int,
        dlq: str,
        now: datetime.datetime,
    ) -> None:
        with self.db.transaction() as tx:
            tx.execute(
                SQL_MOVE_TO_DLQ,
                {
                    "message_id": message_id,
                    "version": version,
                    "dlq": dlq,
                    "now": self.dm.dump_timestamp(now),
                },
            )

    def find_message(self, message_id: str) -> core.Message:
        with self.db.transaction(read_only=True) as tx:
            try:
                row = tx.query_row(SQL_FIND_MESSAGE, {"id": message_id})
            except sqlite.NoRowsError:
                raise core.MessageNotFoundError(message_id) from None
            return self._collect_message(row)

    def _collect_queue(self, row) -> core.Queue:
        return core.Queue(
            name=row[0],
            dead_letter_queue=row[1],
            visibility_timeout=row[2],
            created_at=self.dm.load_timestamp(row[3]),
        )

    def _collect_message(self, row) -> core.Message:
        return core.Message(
            id=row[0],
            queue=row[1],
            payload=row[2],
            created_at=self.dm.load_timestamp(row[3]),
            version=row[4],
        )
