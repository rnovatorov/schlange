import argparse

import schlange

from .command import Command
from .subparsers import Subparsers


class ScheduleDeleteCommand(Command):

    @staticmethod
    def register(schedule_subparsers: Subparsers) -> None:
        schedule_delete_parser = schedule_subparsers.add_parser(
            "delete", formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
        schedule_delete_parser.add_argument("schedule_id")

    @staticmethod
    def run(args: argparse.Namespace) -> None:
        with schlange.new(args.database_path) as sch:
            sch.delete_schedule(args.schedule_id)
