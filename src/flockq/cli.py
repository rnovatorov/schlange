import argparse
import json
import logging
import pathlib
import random
import sys
import time
from typing import BinaryIO

from .client import Client


def cli():
    args = parse_args()
    configure_logging(level=logging.DEBUG if args.verbose else logging.INFO)
    match args.command:
        case "task":
            match args.task_command:
                case "create":
                    create_task(
                        data_dir=args.data_dir,
                        args_file=args.args_file,
                        delay=args.delay,
                    )
                case "inspect":
                    inspect_task(
                        data_dir=args.data_dir,
                        task_id=args.task_id,
                    )
                case "dummy_exec":
                    dummy_exec_task(
                        data_dir=args.data_dir,
                    )
                case _:
                    raise NotImplementedError(args.command)
        case _:
            raise NotImplementedError(args.command)


def create_task(data_dir: pathlib.Path, args_file: BinaryIO, delay: float) -> None:
    for line in args_file:
        args = json.loads(line)
        client = Client.new(data_dir, executor=None)
        task = client.create_task(args, delay=delay)
        print(task.id)


def inspect_task(data_dir: pathlib.Path, task_id: str) -> None:
    client = Client.new(data_dir, executor=None)
    task = client.find_task(task_id)
    if task is None:
        print("not found")
        exit(1)
    print(f"id: {task.id}")
    print(f"args: {task.args}")
    print(f"ready_at: {task.ready_at}")
    print(f"state: {task.state}")
    print(f"executions: {task.executions}")
    print(f"retry_policy: {task.retry_policy}")


def dummy_exec_task(data_dir: pathlib.Path) -> None:
    def executor(task):
        time.sleep(random.random())
        if random.random() < 0.5:
            raise RuntimeError("oops")

    with Client.new(data_dir, executor=executor):
        try:
            time.sleep(60 * 60)
        except KeyboardInterrupt:
            return


def configure_logging(level: int):
    logger = logging.getLogger()
    logger.setLevel(level)
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s.%(msecs)dZ [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    formatter.converter = time.gmtime
    handler.formatter = formatter
    logger.addHandler(handler)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="flockq")
    parser.add_argument(
        "-d",
        "--data-dir",
        type=pathlib.Path,
        required=True,
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    task_parser = subparsers.add_parser("task")
    task_subparsers = task_parser.add_subparsers(dest="task_command", required=True)
    task_create_parser = task_subparsers.add_parser("create")
    task_create_parser.add_argument(
        "--delay",
        type=float,
        default=0,
    )
    task_create_parser.add_argument(
        "args_file",
        nargs="?",
        type=argparse.FileType("rb"),
        default=sys.stdin,
    )
    task_inspect_parser = task_subparsers.add_parser("inspect")
    task_inspect_parser.add_argument(
        "task_id",
        type=str,
    )
    task_subparsers.add_parser("dummy_exec")
    return parser.parse_args()
