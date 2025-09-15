import argparse

from .command import Command
from .subparsers import Subparsers
from .task_create_command import TaskCreateCommand
from .task_delete_command import TaskDeleteCommand
from .task_inspect_command import TaskInspectCommand
from .task_list_command import TaskListCommand
from .task_reactivate_command import TaskReactivateCommand


class TaskCommand(Command):

    @staticmethod
    def register(subparsers: Subparsers) -> None:
        task_parser = subparsers.add_parser(
            "task", formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
        task_subparsers = task_parser.add_subparsers(dest="task_command", required=True)
        for command in [
            TaskCreateCommand,
            TaskInspectCommand,
            TaskListCommand,
            TaskDeleteCommand,
            TaskReactivateCommand,
        ]:
            command.register(task_subparsers)

    @staticmethod
    def run(args: argparse.Namespace) -> None:
        match args.task_command:
            case "create":
                TaskCreateCommand.run(args)
            case "inspect":
                TaskInspectCommand.run(args)
            case "list":
                TaskListCommand.run(args)
            case "delete":
                TaskDeleteCommand.run(args)
            case "reactivate":
                TaskReactivateCommand.run(args)
            case _:
                raise NotImplementedError(args.task_command)
