import argparse

import schlange

from .command import Command
from .subparsers import Subparsers


class TaskListCommand(Command):

    @staticmethod
    def register(task_subparsers: Subparsers) -> None:
        task_list_parser = task_subparsers.add_parser(
            "list", formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
        task_list_parser.add_argument(
            "-s",
            "--state",
            type=schlange.TaskState,
            choices=["ACTIVE", "SUCCEEDED", "FAILED"],
        )

    @staticmethod
    def run(args: argparse.Namespace) -> None:
        with schlange.new(args.database_path) as sch:
            for task in sch.tasks(state=args.state):
                print(f"id: {task.id}")
                print(f"state: {task.state}")
