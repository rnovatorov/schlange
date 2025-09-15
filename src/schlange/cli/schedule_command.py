import argparse

from .command import Command
from .schedule_delete_command import ScheduleDeleteCommand
from .schedule_inspect_command import ScheduleInspectCommand
from .subparsers import Subparsers


class ScheduleCommand(Command):

    @staticmethod
    def register(subparsers: Subparsers) -> None:
        schedule_parser = subparsers.add_parser(
            "schedule", formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
        schedule_subparsers = schedule_parser.add_subparsers(
            dest="schedule_command", required=True
        )
        for command in [
            ScheduleDeleteCommand,
            ScheduleInspectCommand,
        ]:
            command.register(schedule_subparsers)

    @staticmethod
    def run(args: argparse.Namespace) -> None:
        match args.schedule_command:
            case "delete":
                ScheduleDeleteCommand.run(args)
            case "inspect":
                ScheduleInspectCommand.run(args)
            case _:
                raise NotImplementedError(args.schedule_command)
