import argparse
import threading
import time

import schlange
from schlange.services.execution import core as execution_core

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
            default=4,
            help="number of concurrent consumers per kind",
        )

    @staticmethod
    def run(args: argparse.Namespace) -> None:
        lock = threading.Lock()
        tasks_handled = 0
        done = threading.Event()

        def handle_bench(execution: execution_core.TaskExecution) -> None:
            nonlocal tasks_handled
            with lock:
                tasks_handled += 1
            if tasks_handled == args.tasks:
                done.set()

        with schlange.new(
            task_database_path=args.task_database_path,
            schedule_database_path=args.schedule_database_path,
            handlers={"bench": handle_bench},
            consumers_per_kind=args.workers,
        ) as sch:
            started_creating_tasks_at = time.time()
            for i in range(args.tasks):
                sch.create_task(args={}, kind="bench", delay=0)
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
            f"handling {args.tasks} tasks using {args.workers} consumers took {handling_tasks_took:.2f} seconds, rate is {args.tasks/handling_tasks_took:.2f} tasks per second"
        )
