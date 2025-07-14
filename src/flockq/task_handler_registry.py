import threading
from typing import Dict

from .task_handler import TaskHandler


class TaskHandlerRegistry:

    def __init__(self) -> None:
        self.task_handlers: Dict[str, TaskHandler] = {}
        self.lock = threading.Lock()

    def register_task_handler(self, task_handler: TaskHandler):
        with self.lock:
            self.task_handlers[task_handler.task_kind()] = task_handler

    def task_handler(self, task_kind: str) -> TaskHandler:
        with self.lock:
            return self.task_handlers[task_kind]

    def task_kinds(self):
        with self.lock:
            return set(self.task_handlers.keys())
