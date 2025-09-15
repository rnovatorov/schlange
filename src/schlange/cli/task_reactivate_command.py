import argparse

import schlange

from .command import Command
from .subparsers import Subparsers


class TaskReactivateCommand(Command):

    @staticmethod
    def register(task_subparsers: Subparsers) -> None:
        task_reactivate_parser = task_subparsers.add_parser(
            "reactivate", formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
        task_reactivate_parser.add_argument("task_id")

    @staticmethod
    def run(args: argparse.Namespace) -> None:
        with schlange.new(args.database_path) as sch:
            sch.reactivate_task(args.task_id)
