import argparse
import json
import sys

import schlange

from .command import Command
from .subparsers import Subparsers


class TaskCreateCommand(Command):

    @staticmethod
    def register(task_subparsers: Subparsers) -> None:
        task_create_parser = task_subparsers.add_parser(
            "create", formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
        task_create_parser.add_argument(
            "--delay", type=float, default=0, help="delay before start"
        )
        task_create_parser.add_argument(
            "args_file",
            nargs="?",
            type=argparse.FileType("rb"),
            default=sys.stdin,
            help="path to a file with tasks args",
        )

    @staticmethod
    def run(args: argparse.Namespace) -> None:
        with schlange.new(args.database_path) as sch:
            for line in args.args_file:
                task_args = json.loads(line)
                sch.create_task(task_args, delay=args.delay)
