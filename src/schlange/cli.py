import argparse
import json
import logging
import sys
import threading
import time
from typing import BinaryIO

from schlange import core

from .schlange import DEFAULT_EXECUTION_WORKER_PROCESSES, Schlange


def main() -> None:
    args = parse_args()
    configure_logging(level=logging.DEBUG if args.verbose else logging.INFO)
    match args.command:
        case "task":
            match args.task_command:
                case "create":
                    create_task(
                        url=args.url,
                        args_file=args.args_file,
                        delay=args.delay,
                    )
                case "inspect":
                    inspect_task(
                        url=args.url,
                        task_id=args.task_id,
                    )
                case _:
                    raise NotImplementedError(args.command)
        case "bench":
            bench(
                url=args.url,
                tasks=args.tasks,
                workers=args.workers,
            )
        case _:
            raise NotImplementedError(args.command)


def bench(url: str, tasks: int, workers: int) -> None:
    lock = threading.Lock()
    tasks_handled = 0
    done = threading.Event()

    def handle_task(task: core.Task) -> None:
        nonlocal tasks_handled
        with lock:
            tasks_handled += 1
        if tasks_handled == tasks:
            done.set()

    with Schlange.new(
        url, task_handler=handle_task, execution_worker_processes=workers
    ) as q:
        started_creating_tasks_at = time.time()
        for i in range(tasks):
            q.create_task(args={}, delay=0)
        finished_creating_tasks_at = time.time()
        creating_tasks_took = finished_creating_tasks_at - started_creating_tasks_at

        started_handling_tasks_at = time.time()
        with q:
            done.wait()
        finished_handling_tasks_at = time.time()
        handling_tasks_took = finished_handling_tasks_at - started_handling_tasks_at

    print(
        f"creating {tasks} tasks using 1 workers took {creating_tasks_took:.2f} seconds, rate is {tasks/creating_tasks_took:.2f} tasks per second"
    )
    print(
        f"handling {tasks} tasks using {workers} workers took {handling_tasks_took:.2f} seconds, rate is {tasks/handling_tasks_took:.2f} tasks per second"
    )


def create_task(url: str, args_file: BinaryIO, delay: float) -> None:
    with Schlange.new(url) as q:
        for line in args_file:
            args = json.loads(line)
            q.create_task(args, delay=delay)


def inspect_task(url: str, task_id: str) -> None:
    with Schlange.new(url) as q:
        task = q.task(task_id)
        if task is None:
            print("not found")
            exit(1)
        print(f"id: {task.id}")
        print(f"args: {task.args}")
        print(f"ready_at: {task.ready_at}")
        print(f"state: {task.state}")
        print(f"executions: {task.executions}")
        print(f"retry_policy: {task.retry_policy}")


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
    parser = argparse.ArgumentParser(prog="schlange")
    parser.add_argument(
        "-u",
        "--url",
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

    bench_parser = subparsers.add_parser("bench")
    bench_parser.add_argument(
        "-t",
        "--tasks",
        type=int,
        default=1000,
    )
    bench_parser.add_argument(
        "-w",
        "--workers",
        type=int,
        default=DEFAULT_EXECUTION_WORKER_PROCESSES,
    )

    return parser.parse_args()
