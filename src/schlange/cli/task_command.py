import argparse

from .command import Command
from .subparsers import Subparsers
from .task_create_command import TaskCreateCommand
from .task_inspect_command import TaskInspectCommand


class TaskCommand(Command):

    @staticmethod
    def register(subparsers: Subparsers) -> None:
        task_parser = subparsers.add_parser("task")
        task_subparsers = task_parser.add_subparsers(dest="task_command", required=True)
        for command in [
            TaskCreateCommand,
            TaskInspectCommand,
        ]:
            command.register(task_subparsers)

    @staticmethod
    def run(args: argparse.Namespace) -> None:
        match args.task_command:
            case "create":
                TaskCreateCommand.run(args)
            case "inspect":
                TaskInspectCommand.run(args)
            case _:
                raise NotImplementedError(args.task_command)
