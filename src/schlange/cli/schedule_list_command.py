import argparse
import json

import schlange

from .command import Command
from .data_mapper import DataMapper
from .subparsers import Subparsers


class ScheduleListCommand(Command):

    @staticmethod
    def register(task_subparsers: Subparsers) -> None:
        schedule_list_parser = task_subparsers.add_parser(
            "list", formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
        enabled = schedule_list_parser.add_mutually_exclusive_group()
        enabled.add_argument("-e", "--enabled", action="store_true")
        enabled.add_argument("-d", "--disabled", action="store_true")

    @staticmethod
    def run(args: argparse.Namespace) -> None:
        data_mapper = DataMapper()
        with schlange.new(args.database_path) as sch:
            for schedule in sch.schedules(
                enabled=True if args.enabled else False if args.disabled else None
            ):
                dto = data_mapper.dump_schedule(schedule)
                print(json.dumps(dto, indent=4))
