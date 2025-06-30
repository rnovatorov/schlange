import contextlib
import dataclasses
import fcntl
import json
import os
import pathlib
import tempfile
from typing import BinaryIO, Generator, List

from .errors import TaskLocked, TaskNotFound
from .file_system_data_mapper import FileSystemDataMapper
from .task import Task
from .task_events import TaskEvent


@dataclasses.dataclass
class FileSystemTaskRepository:

    data_dir_path: pathlib.Path

    @classmethod
    def new(cls, data_dir_path: pathlib.Path) -> "FileSystemTaskRepository":
        try:
            os.mkdir(data_dir_path)
        except FileExistsError:
            pass
        return cls(data_dir_path=data_dir_path)

    def add_task(self, task: Task) -> None:
        with tempfile.NamedTemporaryFile(
            dir=self.data_dir_path,
            delete=False,
            suffix=".tmp",
        ) as temp_file:
            self._write_change_log(task.change_log, temp_file)  # type: ignore
        file_path = self.data_dir_path / f"{task.id}.json"
        os.replace(temp_file.name, file_path)

    def list_tasks(self) -> Generator[str]:
        for path in self.data_dir_path.iterdir():
            if path.name.endswith(".json"):
                (task_id, _) = path.name.split(".")
                yield task_id

    @contextlib.contextmanager
    def update_task(self, task_id: str) -> Generator[Task]:
        path = self.data_dir_path / f"{task_id}.json"
        try:
            file = path.open("r+b")
        except FileNotFoundError:
            raise TaskNotFound()
        with file:
            try:
                fcntl.flock(file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise TaskLocked()
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
