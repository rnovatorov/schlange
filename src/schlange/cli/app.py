import argparse
import logging
import time

import schlange

from .bench_command import BenchCommand
from .schedule_command import ScheduleCommand
from .stress_command import StressCommand
from .task_command import TaskCommand


class App:

    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args

    @classmethod
    def new(cls) -> "App":
        parser = argparse.ArgumentParser(
            prog="schlange", formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
        parser.add_argument(
            "-d", "--database-path", default=schlange.DEFAULT_DATABASE_PATH
        )
        parser.add_argument("-v", "--verbose", action="store_true")
        subparsers = parser.add_subparsers(dest="command", required=True)
        for command in [
            TaskCommand,
            ScheduleCommand,
            BenchCommand,
            StressCommand,
        ]:
            command.register(subparsers)
        return cls(args=parser.parse_args())

    def run(self) -> None:
        configure_logging(level=logging.DEBUG if self.args.verbose else logging.INFO)
        match self.args.command:
            case "task":
                TaskCommand.run(self.args)
            case "schedule":
                ScheduleCommand.run(self.args)
            case "bench":
                BenchCommand.run(self.args)
            case "stress":
                StressCommand.run(self.args)
            case _:
                raise NotImplementedError(self.args.command)


def configure_logging(level: int):
    logger = logging.getLogger()
    logger.setLevel(level)
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s.%(msecs)dZ [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    formatter.converter = time.gmtime
    handler.formatter = formatter
    logger.addHandler(handler)
