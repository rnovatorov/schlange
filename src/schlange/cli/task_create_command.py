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
            "args_file",
            nargs="?",
            type=argparse.FileType("rb"),
            default=sys.stdin,
            help="path to a file with tasks args",
        )
        task_create_parser.add_argument(
            "--delay", type=float, default=0, help="delay before initial start"
        )
        task_create_parser.add_argument(
            "--retry-policy-initial-delay",
            type=float,
            default=schlange.DEFAULT_RETRY_POLICY.initial_delay,
            help="delay before the first retry",
        )
        task_create_parser.add_argument(
            "--retry-policy-backoff-factor",
            type=float,
            default=schlange.DEFAULT_RETRY_POLICY.backoff_factor,
            help="backoff factor to use to calculate delay between retries",
        )
        task_create_parser.add_argument(
            "--retry-policy-max-delay",
            type=float,
            default=schlange.DEFAULT_RETRY_POLICY.max_delay,
            help="max delay between retries",
        )
        task_create_parser.add_argument(
            "--retry-policy-max-attempts",
            type=int,
            default=schlange.DEFAULT_RETRY_POLICY.max_attempts,
            help="max number of retry attempts",
        )

    @staticmethod
    def run(args: argparse.Namespace) -> None:
        with schlange.new(args.database_path) as sch:
            for line in args.args_file:
                task_args = json.loads(line)
                retry_policy = schlange.RetryPolicy(
                    initial_delay=args.retry_policy_initial_delay,
                    backoff_factor=args.retry_policy_backoff_factor,
                    max_delay=args.retry_policy_max_delay,
                    max_attempts=args.retry_policy_max_attempts,
                )
                sch.create_task(task_args, delay=args.delay, retry_policy=retry_policy)
