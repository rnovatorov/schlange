import contextlib
import dataclasses
import fcntl
import logging
import os
import pathlib
import tempfile
from typing import Generator

from flockq import core

from .file_system_errors import TaskFilePathInvalidError
from .file_system_task_journal import FileSystemTaskJournal, FileSystemTaskJournalRecord

LOGGER = logging.getLogger(__name__)


@dataclasses.dataclass
class FileSystemTaskRepository:

    data_dir_path: pathlib.Path

    def make_dirs(self) -> None:
        self.data_dir_path.mkdir(exist_ok=True)
        for task_state in core.TaskState:
            task_dir_path = self.task_dir_path(task_state)
            task_dir_path.mkdir(exist_ok=True)

    def make_partition(self, task: core.Task) -> None:
        self.task_dir_partition_path(task.state, task.id).mkdir(exist_ok=True)

    def task_dir_path(self, task_state: core.TaskState) -> pathlib.Path:
        return self.data_dir_path / task_state

    def task_dir_partition_path(
        self, task_state: core.TaskState, task_id: str
    ) -> pathlib.Path:
        return self.task_dir_path(task_state) / task_id[:2]

    def task_file_path(self, task_state: core.TaskState, task_id: str) -> pathlib.Path:
        return self.task_dir_partition_path(task_state, task_id) / f"{task_id}.jsonl"

    def add_task(self, task: core.Task) -> None:
        with tempfile.NamedTemporaryFile(
            dir=self.data_dir_path,
            delete=False,
            suffix=".tmp",
        ) as temp_file:
            record = FileSystemTaskJournalRecord(events=task.change_log)
            FileSystemTaskJournal.write_record(record, temp_file)  # type: ignore
        self.make_partition(task)
        os.replace(temp_file.name, self.task_file_path(task.state, task.id))

    def get_task(self, task_state: core.TaskState, task_id: str) -> core.Task:
        try:
            return self._get_task(task_state, task_id)
        except TaskFilePathInvalidError:
            try:
                self.repair_task_file_path(task_state, task_id)
            except:
                pass
            raise core.TaskNotFoundError()

    def _get_task(self, task_state: core.TaskState, task_id: str) -> core.Task:
        path = self.task_file_path(task_state, task_id)
        try:
            file = path.open("rb")
        except FileNotFoundError:
            raise core.TaskNotFoundError()
        with file:
            journal = FileSystemTaskJournal.read(file)
            task = journal.rehydrate_task(task_id)
            if task.state != task_state:
                raise TaskFilePathInvalidError()
            return task

    def list_tasks(
        self, task_state: core.TaskState, spec: core.TaskSpecification
    ) -> Generator[core.Task]:
        for partition in self.task_dir_path(task_state).iterdir():
            for path in partition.iterdir():
                if not path.name.endswith(".jsonl"):
                    continue
                (task_id, _) = path.name.split(".")
                try:
                    task = self.get_task(task_state, task_id)
                except core.TaskNotFoundError:
                    continue
                if spec.is_satisfied_by(task):
                    yield task

    def delete_task(self, task_state: core.TaskState, task_id: str) -> None:
        path = self.task_file_path(task_state, task_id)
        try:
            path.unlink()
        except FileNotFoundError:
            raise core.TaskNotFoundError()

    def repair_task_file_path(self, task_state: core.TaskState, task_id: str) -> None:
        with self.update_task(task_state, task_id):
            pass

    @contextlib.contextmanager
    def update_task(
        self, task_state: core.TaskState, task_id: str
    ) -> Generator[core.Task]:
        path = self.task_file_path(task_state, task_id)
        try:
            file = path.open("r+b")
        except FileNotFoundError:
            raise core.TaskNotFoundError()
        with file:
            try:
                fcntl.flock(file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise core.TaskLockedError()
            journal = FileSystemTaskJournal.read(file, repair=True)
            task = journal.rehydrate_task(task_id)
            if task.state != task_state:
                self.make_partition(task)
                try:
                    os.replace(path, self.task_file_path(task.state, task.id))
                except FileNotFoundError:
                    pass
                raise core.TaskNotFoundError()
            yield task
            if task.change_log:
                record = FileSystemTaskJournalRecord(events=task.change_log)
                FileSystemTaskJournal.write_record(record, file)
            if task.state != task_state:
                self.make_partition(task)
                file.flush()
                os.replace(path, self.task_file_path(task.state, task.id))
