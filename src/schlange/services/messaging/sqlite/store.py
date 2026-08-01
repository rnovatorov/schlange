import datetime
import sqlite3

from schlange.internal import sqlite
from schlange.services.messaging import core

SQL_INSERT_QUEUE = """
    INSERT INTO queues (name, dead_letter_queue, max_delivery_count, created_at)
    VALUES (:name, :dead_letter_queue, :max_delivery_count, :created_at)
"""

SQL_FIND_QUEUE = """
    SELECT name, dead_letter_queue, max_delivery_count, created_at
    FROM queues
    WHERE name = :name
"""

SQL_PUBLISH = """
    INSERT INTO messages
        (id, queue, payload, visibility_timeout, delivery_count,
         visible_at, created_at, version)
    VALUES
        (:id, :queue, :payload, :visibility_timeout, 0,
         :created_at, :created_at, 0)
"""

SQL_CLAIM = """
    UPDATE messages
    SET visible_at = :now + messages.visibility_timeout,
        delivery_count = delivery_count + 1,
        version = version + 1
    WHERE id = (
        SELECT id
        FROM messages
        WHERE queue = :queue AND visible_at <= :now
        ORDER BY created_at
        LIMIT 1
    )
    RETURNING id, queue, payload, visibility_timeout, delivery_count,
              created_at, version
"""

SQL_DELETE_MESSAGE = """
    DELETE FROM messages
    WHERE id = :message_id AND version = :version
"""

SQL_REQUEUE = """
    UPDATE messages
    SET visible_at = :now,
        version = version + 1
    WHERE id = :message_id AND version = :version
"""

SQL_MOVE_TO_DLQ = """
    UPDATE messages
    SET queue = :dlq_queue,
        delivery_count = 0,
        visible_at = :now,
        version = version + 1
    WHERE id = :message_id AND version = :version
"""

SQL_FIND_MESSAGE = """
    SELECT id, queue, payload, visibility_timeout, delivery_count,
           created_at, version
    FROM messages
    WHERE id = :id
"""


class Store:

    def __init__(self, db: sqlite.Database) -> None:
        self.db = db
        self.dm = sqlite.DataMapper()

    def create_queue(
        self,
        name: str,
        dead_letter_queue: str | None,
        max_delivery_count: int,
        created_at: datetime.datetime,
    ) -> None:
        with self.db.transaction() as tx:
            try:
                tx.execute(
                    SQL_INSERT_QUEUE,
                    {
                        "name": name,
                        "dead_letter_queue": dead_letter_queue,
                        "max_delivery_count": max_delivery_count,
                        "created_at": self.dm.dump_timestamp(created_at),
                    },
                )
            except sqlite3.IntegrityError as e:
                if e.sqlite_errorname == "SQLITE_CONSTRAINT_PRIMARYKEY":
                    raise core.QueueAlreadyExistsError(name) from None
                if e.sqlite_errorname == "SQLITE_CONSTRAINT_FOREIGNKEY":
                    raise core.QueueNotFoundError(dead_letter_queue) from None
                raise

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
        visibility_timeout: float,
        created_at: datetime.datetime,
    ) -> None:
        epoch = self.dm.dump_timestamp(created_at)
        with self.db.transaction() as tx:
            try:
                tx.execute(
                    SQL_PUBLISH,
                    {
                        "id": message_id,
                        "queue": queue,
                        "payload": payload,
                        "visibility_timeout": visibility_timeout,
                        "created_at": epoch,
                    },
                )
            except sqlite3.IntegrityError as e:
                if e.sqlite_errorname == "SQLITE_CONSTRAINT_FOREIGNKEY":
                    raise core.QueueNotFoundError(queue) from None
                raise

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

    def requeue_message(
        self,
        message_id: str,
        version: int,
        now: datetime.datetime,
    ) -> None:
        with self.db.transaction(synchronous=False) as tx:
            tx.execute(
                SQL_REQUEUE,
                {
                    "message_id": message_id,
                    "version": version,
                    "now": self.dm.dump_timestamp(now),
                },
            )

    def move_message_to_dlq(
        self,
        message_id: str,
        version: int,
        dlq_queue: str,
        now: datetime.datetime,
    ) -> None:
        with self.db.transaction(synchronous=False) as tx:
            tx.execute(
                SQL_MOVE_TO_DLQ,
                {
                    "message_id": message_id,
                    "version": version,
                    "dlq_queue": dlq_queue,
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
            max_delivery_count=row[2],
            created_at=self.dm.load_timestamp(row[3]),
        )

    def _collect_message(self, row) -> core.Message:
        return core.Message(
            id=row[0],
            queue=row[1],
            payload=row[2],
            visibility_timeout=row[3],
            delivery_count=row[4],
            created_at=self.dm.load_timestamp(row[5]),
            version=row[6],
        )
