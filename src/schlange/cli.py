import argparse
import json
import logging
import random
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
                    raise NotImplementedError(args.task_command)
        case "schedule":
            match args.schedule_command:
                case "inspect":
                    inspect_schedule(
                        url=args.url,
                        schedule_id=args.schedule_id,
                    )
                case "delete":
                    delete_schedule(
                        url=args.url,
                        schedule_id=args.schedule_id,
                    )
                case _:
                    raise NotImplementedError(args.schedule_command)
        case "bench":
            bench(
                url=args.url,
                tasks=args.tasks,
                workers=args.workers,
            )
        case "stress":
            stress(
                url=args.url,
                schedules=args.schedules,
                interval=args.interval,
                workers=args.workers,
                min_task_duration=args.min_task_duration,
                max_task_duration=args.max_task_duration,
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


def stress(
    url: str,
    schedules: int,
    interval: float,
    workers: int,
    min_task_duration: float,
    max_task_duration: float,
) -> None:
    lock = threading.Lock()
    tasks_handled = 0

    def handle_task(task: core.Task) -> None:
        duration = random.random() * (max_task_duration - min_task_duration)
        time.sleep(duration)
        nonlocal tasks_handled
        with lock:
            tasks_handled += 1

    with Schlange.new(
        url, task_handler=handle_task, execution_worker_processes=workers
    ) as queue:
        for i in range(schedules):
            queue.create_schedule(task_args={}, interval=interval)

        started_at = time.time()
        with queue:
            try:
                threading.Event().wait()
            except KeyboardInterrupt:
                pass
        finished_at = time.time()
        duration = finished_at - started_at

    print(
        f"handling {schedules} schedules each with {interval} interval using {workers} workers took {duration:.2f} seconds, rate is {tasks_handled/duration:.2f} tasks per second"
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


def inspect_schedule(url: str, schedule_id: str) -> None:
    with Schlange.new(url) as q:
        schedule = q.schedule(schedule_id)
        print(schedule)


def delete_schedule(url: str, schedule_id: str) -> None:
    with Schlange.new(url) as q:
        q.delete_schedule(schedule_id)


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
    task_inspect_parser.add_argument("task_id")

    schedule_parser = subparsers.add_parser("schedule")
    schedule_subparsers = schedule_parser.add_subparsers(
        dest="schedule_command", required=True
    )
    schedule_delete_parser = schedule_subparsers.add_parser("delete")
    schedule_delete_parser.add_argument("schedule_id")
    schedule_inspect_parser = schedule_subparsers.add_parser("inspect")
    schedule_inspect_parser.add_argument("schedule_id")

    bench_parser = subparsers.add_parser("bench")
    bench_parser.add_argument(
        "-t",
        "--tasks",
        type=int,
        default=5000,
    )
    bench_parser.add_argument(
        "-w",
        "--workers",
        type=int,
        default=DEFAULT_EXECUTION_WORKER_PROCESSES,
    )

    stress_parser = subparsers.add_parser("stress")
    stress_parser.add_argument(
        "-s",
        "--schedules",
        type=int,
        default=10,
    )
    stress_parser.add_argument(
        "-i",
        "--interval",
        type=float,
        default=1,
    )
    stress_parser.add_argument(
        "-w",
        "--workers",
        type=int,
        default=DEFAULT_EXECUTION_WORKER_PROCESSES,
    )
    stress_parser.add_argument(
        "--min-task-duration",
        type=float,
        default=0,
    )
    stress_parser.add_argument(
        "--max-task-duration",
        type=float,
        default=0,
    )

    return parser.parse_args()
