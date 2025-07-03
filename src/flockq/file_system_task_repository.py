import contextlib
import dataclasses
import fcntl
import json
import logging
import os
import pathlib
import tempfile
from typing import BinaryIO, Generator, List

from .errors import TaskLockedError, TaskNotFoundError
from .file_system_data_mapper import FileSystemDataMapper
from .task import Task
from .task_events import TaskEvent
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
            self._write_change_log(task.change_log, temp_file)  # type: ignore
        file_path = self.data_dir_path / f"{task.id}.json"
        os.replace(temp_file.name, file_path)

    def get_task(self, task_id: str) -> Task:
        path = self.data_dir_path / f"{task_id}.json"
        try:
            file = path.open("r+b")
        except FileNotFoundError:
            raise TaskNotFoundError()
        with file:
            change_log = self._read_change_log(file)
            return Task.rehydrate(id=task_id, events=change_log)

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
                change_log = self._read_change_log(file)
                task = Task.rehydrate(id=task_id, events=change_log)
                if spec.is_satisfied_by(task):
                    yield task

    def delete_task(self, task_id: str) -> None:
        path = self.data_dir_path / f"{task_id}.json"
        # NOTE: It would be dangerous to delete a task that's being worked on.
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
            change_log = self._read_change_log(file, repair=True)
            task = Task.rehydrate(id=task_id, events=change_log)
            yield task
            if task.change_log:
                self._write_change_log(task.change_log, file)

    def _write_change_log(self, change_log: List[TaskEvent], file: BinaryIO) -> None:
        assert change_log
        data = self._dump_task_events(change_log)
        file.write(data + b"\n")
        file.flush()

    def _read_change_log(self, file: BinaryIO, repair: bool = False) -> List[TaskEvent]:
        change_log = []
        while True:
            pos = file.tell()
            line = file.readline()
            if not line:
                break
            try:
                events = self._load_task_events(line)
            except json.decoder.JSONDecodeError:
                if repair:
                    file.seek(pos)
                    file.truncate()
                break
            change_log.extend(events)
        assert change_log
        return change_log

    def _dump_task_events(self, events: List[TaskEvent]) -> bytes:
        dto = FileSystemDataMapper.dump_task_events(events)
        return json.dumps(dto).encode()

    def _load_task_events(self, data: bytes) -> List[TaskEvent]:
        dto = json.loads(data)
        return FileSystemDataMapper.load_task_events(dto)
