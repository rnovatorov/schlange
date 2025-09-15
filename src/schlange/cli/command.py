import abc
import argparse

from .subparsers import Subparsers


class Command(abc.ABC):

    @staticmethod
    def register(subparsers: Subparsers) -> None:
        pass

    @staticmethod
    @abc.abstractmethod
    def run(args: argparse.Namespace) -> None:
        pass
