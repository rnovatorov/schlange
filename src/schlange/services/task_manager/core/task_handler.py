from typing import Callable

from .task import Task

TaskHandler = Callable[[Task], None]
