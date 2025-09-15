import argparse
import threading
import time

import schlange

from .command import Command
from .subparsers import Subparsers


class BenchCommand(Command):

    @staticmethod
    def register(subparsers: Subparsers) -> None:
        bench_parser = subparsers.add_parser(
            "bench", formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
        bench_parser.add_argument(
            "-t", "--tasks", type=int, default=5000, help="number of tasks to create"
        )
        bench_parser.add_argument(
            "-w",
            "--workers",
            type=int,
            default=schlange.DEFAULT_EXECUTION_WORKER_THREADS,
            help="number of concurrent execution workers",
        )

    @staticmethod
    def run(args: argparse.Namespace) -> None:
        lock = threading.Lock()
        tasks_handled = 0
        done = threading.Event()

        def handle_task(task: schlange.Task) -> None:
            nonlocal tasks_handled
            with lock:
                tasks_handled += 1
            if tasks_handled == args.tasks:
                done.set()

        with schlange.new(
            args.database_path,
            task_handler=handle_task,
            execution_worker_threads=args.workers,
        ) as sch:
            started_creating_tasks_at = time.time()
            for i in range(args.tasks):
                sch.create_task(args={}, delay=0)
            finished_creating_tasks_at = time.time()
            creating_tasks_took = finished_creating_tasks_at - started_creating_tasks_at

            started_handling_tasks_at = time.time()
            with sch:
                done.wait()
            finished_handling_tasks_at = time.time()
            handling_tasks_took = finished_handling_tasks_at - started_handling_tasks_at

        print(
            f"creating {args.tasks} tasks using 1 workers took {creating_tasks_took:.2f} seconds, rate is {args.tasks/creating_tasks_took:.2f} tasks per second"
        )
        print(
            f"handling {args.tasks} tasks using {args.workers} workers took {handling_tasks_took:.2f} seconds, rate is {args.tasks/handling_tasks_took:.2f} tasks per second"
        )
