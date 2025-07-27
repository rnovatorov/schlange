import contextlib
import dataclasses
import fcntl
import logging
import os
import pathlib
import tempfile
from typing import Generator

from .errors import TaskFilePathInvalidError, TaskLockedError, TaskNotFoundError
from .file_system_task_journal import FileSystemTaskJournal, FileSystemTaskJournalRecord
from .task import Task
from .task_specification import TaskSpecification
from .task_state import TaskState

LOGGER = logging.getLogger(__name__)


@dataclasses.dataclass
class FileSystemTaskRepository:

    data_dir_path: pathlib.Path

    def make_dirs(self) -> None:
        self.data_dir_path.mkdir(exist_ok=True)
        for task_state in TaskState:
            task_dir_path = self.task_dir_path(task_state)
            task_dir_path.mkdir(exist_ok=True)

    def task_dir_path(self, task_state: TaskState) -> pathlib.Path:
        return self.data_dir_path / task_state

    def task_file_path(self, task_state: TaskState, task_id: str) -> pathlib.Path:
        return self.task_dir_path(task_state) / f"{task_id}.jsonl"

    def add_task(self, task: Task) -> None:
        with tempfile.NamedTemporaryFile(
            dir=self.data_dir_path,
            delete=False,
            suffix=".tmp",
        ) as temp_file:
            record = FileSystemTaskJournalRecord(events=task.change_log)
            FileSystemTaskJournal.write_record(record, temp_file)  # type: ignore
        os.replace(temp_file.name, self.task_file_path(task.state, task.id))

    def get_task(self, task_state: TaskState, task_id: str) -> Task:
        try:
            return self._get_task(task_state, task_id)
        except TaskFilePathInvalidError:
            try:
                self.repair_task_file_path(task_state, task_id)
            except:
                pass
            raise TaskNotFoundError()

    def _get_task(self, task_state: TaskState, task_id: str) -> Task:
        path = self.task_file_path(task_state, task_id)
        try:
            file = path.open("rb")
        except FileNotFoundError:
            raise TaskNotFoundError()
        with file:
            journal = FileSystemTaskJournal.read(file)
            task = journal.rehydrate_task(task_id)
            if task.state != task_state:
                raise TaskFilePathInvalidError()
            return task

    def list_tasks(
        self, task_state: TaskState, spec: TaskSpecification
    ) -> Generator[Task]:
        for path in self.task_dir_path(task_state).iterdir():
            if not path.name.endswith(".jsonl"):
                continue
            (task_id, _) = path.name.split(".")
            try:
                task = self.get_task(task_state, task_id)
            except TaskNotFoundError:
                continue
            if spec.is_satisfied_by(task):
                yield task

    def delete_task(self, task_state: TaskState, task_id: str) -> None:
        path = self.task_file_path(task_state, task_id)
        try:
            path.unlink()
        except FileNotFoundError:
            raise TaskNotFoundError()

    def repair_task_file_path(self, task_state: TaskState, task_id: str) -> None:
        with self.update_task(task_state, task_id):
            pass

    @contextlib.contextmanager
    def update_task(self, task_state: TaskState, task_id: str) -> Generator[Task]:
        path = self.task_file_path(task_state, task_id)
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
            if task.state != task_state:
                try:
                    os.replace(path, self.task_file_path(task.state, task.id))
                except FileNotFoundError:
                    pass
                raise TaskNotFoundError()
            yield task
            if task.change_log:
                record = FileSystemTaskJournalRecord(events=task.change_log)
                FileSystemTaskJournal.write_record(record, file)
            if task.state != task_state:
                os.replace(path, self.task_file_path(task.state, task.id))
