"""
>>> main()
Hello Task-0
Hello Task-1
Hello Task-2
Hello Task-3
Hello Task-4
"""

import time

import schlange
from schlange.services.execution import core as execution_core


def handle_sleep(execution: execution_core.TaskExecution) -> None:
    print("Hello", execution.args["name"])
    time.sleep(1)


def main():
    with schlange.new(handlers={"sleep": handle_sleep}) as sch:
        for i in range(5):
            sch.create_task(args={"name": f"Task-{i}"}, kind="sleep", delay=i)
        with sch:
            time.sleep(5)


if __name__ == "__main__":
    main()
