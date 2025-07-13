import contextlib
import dataclasses
import fcntl
import logging
import os
import pathlib
import tempfile
from typing import Generator

from .errors import TaskLockedError, TaskNotFoundError
from .file_system_task_journal import FileSystemTaskJournal, FileSystemTaskJournalRecord
from .task import Task
from .task_specification import TaskSpecification

LOGGER = logging.getLogger(__name__)


@dataclasses.dataclass
class FileSystemTaskRepository:

    data_dir_path: pathlib.Path

    def add_task(self, task: Task) -> None:
        with tempfile.NamedTemporaryFile(
            dir=self.data_dir_path,
            delete=False,
            suffix=".tmp",
        ) as temp_file:
            record = FileSystemTaskJournalRecord(events=task.change_log)
            FileSystemTaskJournal.write_record(record, temp_file)  # type: ignore
        file_path = self.data_dir_path / f"{task.id}.json"
        os.replace(temp_file.name, file_path)

    def get_task(self, task_id: str) -> Task:
        path = self.data_dir_path / f"{task_id}.json"
        try:
            file = path.open("r+b")
        except FileNotFoundError:
            raise TaskNotFoundError()
        with file:
            journal = FileSystemTaskJournal.read(file)
            return journal.rehydrate_task(task_id)

    def list_tasks(self, spec: TaskSpecification) -> Generator[Task]:
        for path in self.data_dir_path.iterdir():
            if not path.name.endswith(".json"):
                continue
            (task_id, _) = path.name.split(".")
            try:
                file = path.open("rb")
            except FileNotFoundError:
                continue
            with file:
                journal = FileSystemTaskJournal.read(file)
                task = journal.rehydrate_task(task_id)
                if spec.is_satisfied_by(task):
                    yield task

    def delete_task(self, task_id: str) -> None:
        path = self.data_dir_path / f"{task_id}.json"
        # FIXME: Concurrent update can be lost.
        try:
            path.unlink()
        except FileNotFoundError:
            raise TaskNotFoundError()

    @contextlib.contextmanager
    def update_task(self, task_id: str) -> Generator[Task]:
        path = self.data_dir_path / f"{task_id}.json"
        try:
            file = path.open("r+b")
        except FileNotFoundError:
            raise TaskNotFoundError()
        with file:
            try:
                fcntl.flock(file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise TaskLockedError()
            journal = FileSystemTaskJournal.read(file, repair=True)
            task = journal.rehydrate_task(task_id)
            yield task
            if task.change_log:
                record = FileSystemTaskJournalRecord(events=task.change_log)
                FileSystemTaskJournal.write_record(record, file)
