import argparse
import json

import schlange

from .command import Command
from .data_mapper import DataMapper
from .subparsers import Subparsers


class ScheduleInspectCommand(Command):

    @staticmethod
    def register(schedule_subparsers: Subparsers) -> None:
        schedule_inspect_parser = schedule_subparsers.add_parser(
            "inspect", formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
        schedule_inspect_parser.add_argument("schedule_id")

    @staticmethod
    def run(args: argparse.Namespace) -> None:
        data_mapper = DataMapper()
        with schlange.new(args.database_path) as sch:
            schedule = sch.schedule(args.schedule_id)
            dto = data_mapper.dump_schedule(schedule)
            print(json.dumps(dto, indent=4))
