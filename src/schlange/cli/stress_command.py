import argparse
import random
import threading
import time

import schlange

from .command import Command
from .subparsers import Subparsers


class StressCommand(Command):

    @staticmethod
    def register(subparsers: Subparsers) -> None:
        stress_parser = subparsers.add_parser("stress")
        stress_parser.add_argument("-s", "--schedules", type=int, default=10)
        stress_parser.add_argument("-i", "--interval", type=float, default=1)
        stress_parser.add_argument(
            "-w",
            "--workers",
            type=int,
            default=schlange.DEFAULT_EXECUTION_WORKER_THREADS,
        )
        stress_parser.add_argument("--min-task-duration", type=float, default=0)
        stress_parser.add_argument(
            "--max-task-duration",
            type=float,
            default=0,
        )

    @staticmethod
    def run(args: argparse.Namespace) -> None:
        lock = threading.Lock()
        tasks_handled = 0

        def handle_task(task: schlange.Task) -> None:
            duration = random.random() * (
                args.max_task_duration - args.min_task_duration
            )
            time.sleep(duration)
            nonlocal tasks_handled
            with lock:
                tasks_handled += 1

        with schlange.new(
            database_path=args.database_path,
            task_handler=handle_task,
            execution_worker_threads=args.workers,
        ) as sch:
            for i in range(args.schedules):
                sch.create_schedule(task_args={}, interval=args.interval)

            started_at = time.time()
            with sch:
                try:
                    threading.Event().wait()
                except KeyboardInterrupt:
                    pass
            finished_at = time.time()
            duration = finished_at - started_at

        print(
            f"handling {args.schedules} schedules each with {args.interval} interval using {args.workers} workers took {duration:.2f} seconds, rate is {tasks_handled/duration:.2f} tasks per second"
        )
