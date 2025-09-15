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


def handle_task(task: schlange.Task) -> None:
    print("Hello", task.args["name"])
    time.sleep(1)


def main():
    with schlange.new(task_handler=handle_task) as sch:
        for i in range(5):
            sch.create_task(args={"name": f"Task-{i}"}, delay=i)
        with sch:
            time.sleep(5)


if __name__ == "__main__":
    main()
