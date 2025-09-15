import argparse
import json

import schlange

from .command import Command
from .data_mapper import DataMapper
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
        data_mapper = DataMapper()
        with schlange.new(args.database_path) as sch:
            task = sch.task(args.task_id)
            dto = data_mapper.dump_task(task)
            print(json.dumps(dto, indent=4))
