import contextlib
import dataclasses
import fcntl
import json
import os
import pathlib
from typing import Generator, Optional

from .file_system_data_mapper import FileSystemDataMapper
from .task import Task
from .task_specification import TaskSpecification


@dataclasses.dataclass
class FileSystemTaskRepository:

    data_dir_path: pathlib.Path
    json_indent: int

    @classmethod
    def new(
        cls,
        data_dir_path: pathlib.Path,
        json_indent: int = 4,
    ) -> "FileSystemTaskRepository":
        try:
            os.mkdir(data_dir_path)
        except FileExistsError:
            pass
        return cls(
            data_dir_path=data_dir_path,
            json_indent=json_indent,
        )

    def add_task(self, task: Task) -> None:
        file_path = self.data_dir_path / f"{task.id}.json"
        data = self._dump_task(task)
        self._write_file(file_path, data)

    @contextlib.contextmanager
    def update_task(self, spec: TaskSpecification) -> Generator[Optional[Task]]:
        for path in self.data_dir_path.iterdir():
            if path.name.endswith(".tmp"):
                continue
            with path.open("r+b") as f:
                try:
                    fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    continue
                task = self._load_task(f.read())
                if not spec.is_satisfied_by(task):
                    continue
                yield task
                data = self._dump_task(task)
                self._write_file(path, data)
                return
        yield None

    def _dump_task(self, task: Task) -> bytes:
        dto = FileSystemDataMapper.dump_task(task)
        return json.dumps(dto, indent=self.json_indent).encode()

    def _load_task(self, data: bytes) -> Task:
        dto = json.loads(data.decode())
        return FileSystemDataMapper.load_task(dto)

    @staticmethod
    def _write_file(path: pathlib.Path, data: bytes) -> None:
        temp_path = path.parent / f"{path.name}.tmp"
        with temp_path.open("wb") as temp_file:
            temp_file.write(data)
        # NOTE: Although `os.rename` itself is atomic when operating withing a
        # single file system, the "temp" file may be left hanging if there is
        # an interrupt after we create the file but before we rename it.
        os.rename(temp_path, path)
