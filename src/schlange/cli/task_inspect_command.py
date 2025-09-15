import argparse

import schlange

from .command import Command
from .subparsers import Subparsers


class TaskInspectCommand(Command):

    @staticmethod
    def register(task_subparsers: Subparsers) -> None:
        task_inspect_parser = task_subparsers.add_parser(
            "inspect", formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
        task_inspect_parser.add_argument("task_id")

    @staticmethod
    def run(args: argparse.Namespace) -> None:
        with schlange.new(args.database_path) as sch:
            task = sch.task(args.task_id)
            if task is None:
                print("not found")
                exit(1)
            print(f"id: {task.id}")
            print(f"args: {task.args}")
            print(f"ready_at: {task.ready_at}")
            print(f"state: {task.state}")
            print(f"executions: {task.executions}")
            print(f"retry_policy: {task.retry_policy}")
