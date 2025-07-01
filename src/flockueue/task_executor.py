from typing import Callable

from .task import Task

TaskExecutor = Callable[[Task], None]
