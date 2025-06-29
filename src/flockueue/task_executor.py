from typing import Callable

from .task_args import TaskArgs

TaskExecutor = Callable[[str, TaskArgs], None]
