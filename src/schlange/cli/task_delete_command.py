import argparse

import schlange

from .command import Command
from .subparsers import Subparsers


class TaskDeleteCommand(Command):

    @staticmethod
    def register(task_subparsers: Subparsers) -> None:
        task_delete_parser = task_subparsers.add_parser(
            "delete", formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
        task_delete_parser.add_argument("task_id")

    @staticmethod
    def run(args: argparse.Namespace) -> None:
        with schlange.new(args.database_path) as sch:
            sch.delete_task(args.task_id)
