import argparse

import schlange

from .command import Command
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
        with schlange.new(args.database_path) as sch:
            schedule = sch.schedule(args.schedule_id)
            print(schedule)
