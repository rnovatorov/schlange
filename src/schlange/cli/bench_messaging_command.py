import argparse
import datetime
import pathlib
import tempfile
import threading
import time
import uuid

from schlange.internal import sqlite
from schlange.services.messaging import core
from schlange.services.messaging.sqlite import constants
from schlange.services.messaging.sqlite.store import Store

from .command import Command
from .subparsers import Subparsers

QUEUE = "bench"


class BenchMessagingCommand(Command):

    @staticmethod
    def register(subparsers: Subparsers) -> None:
        parser = subparsers.add_parser(
            "bench-messaging",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        parser.add_argument(
            "-m",
            "--messages",
            type=int,
            default=10000,
            help="number of messages to publish",
        )
        parser.add_argument(
            "-c",
            "--consumers",
            type=int,
            default=4,
            help="number of competing consumer threads",
        )
        parser.add_argument(
            "--payload-size",
            type=int,
            default=256,
            help="payload size in bytes",
        )
        parser.add_argument(
            "--visibility-timeout",
            type=float,
            default=300.0,
            help="visibility timeout in seconds",
        )
        parser.add_argument(
            "--db-path",
            type=pathlib.Path,
            default=None,
            help="database path (default: temp file in current directory)",
        )

    @staticmethod
    def run(args: argparse.Namespace) -> None:
        db_path = args.db_path or pathlib.Path(tempfile.mktemp(suffix=".db", dir="."))
        try:
            with sqlite.Database.open(
                path=db_path, read_pool_capacity=max(args.consumers + 1, 4)
            ) as db:
                db.migrate(migrations_path=constants.MIGRATIONS_PATH)
                store = Store(db)
                store.declare_queue(
                    QUEUE, None, args.visibility_timeout,
                    datetime.datetime.now(datetime.UTC),
                )
                pub_time = _publish(store, args.messages, args.payload_size)
                results, con_time = _consume(store, args.consumers)
                _report(args.messages, pub_time, results, con_time, args.consumers)
        finally:
            _cleanup(db_path)


def _publish(store: Store, count: int, payload_size: int) -> float:
    payload = b"x" * payload_size
    t0 = time.time()
    for _ in range(count):
        store.publish_message(
            str(uuid.uuid4()),
            QUEUE,
            payload,
            datetime.datetime.now(datetime.UTC),
        )
    return time.time() - t0


def _consume(
    store: Store, num_workers: int
) -> tuple[dict[int, int], float]:
    results: dict[int, int] = {}
    threads = [
        threading.Thread(
            target=_consume_worker,
            args=(store, i, results),
        )
        for i in range(num_workers)
    ]
    t0 = time.time()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return results, time.time() - t0


def _consume_worker(
    store: Store,
    worker_id: int,
    results: dict[int, int],
) -> None:
    count = 0
    while True:
        try:
            msg = store.claim_message(
                QUEUE,
                datetime.datetime.now(datetime.UTC),
            )
        except core.NoMessagesAvailable:
            break
        count += 1
        store.delete_message(msg.id, msg.version)
    results[worker_id] = count


def _report(
    total_published: int,
    pub_time: float,
    results: dict[int, int],
    con_time: float,
    num_consumers: int,
) -> None:
    total_consumed = sum(results.values())
    print(
        f"publish: {total_published} messages in {pub_time:.2f}s "
        f"({total_published / pub_time:.0f} msgs/sec)"
    )
    print(
        f"consume: {total_consumed} messages in {con_time:.2f}s "
        f"({total_consumed / con_time:.0f} msgs/sec) with {num_consumers} consumers"
    )
    for wid in sorted(results):
        print(f"  consumer {wid}: {results[wid]} messages")


def _cleanup(db_path: pathlib.Path) -> None:
    db_path.unlink(missing_ok=True)
    for suffix in ["-wal", "-shm"]:
        pathlib.Path(str(db_path) + suffix).unlink(missing_ok=True)
