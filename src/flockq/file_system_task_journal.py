import dataclasses
import json
import os
from typing import BinaryIO, Generator, List

from .file_system_data_mapper import FileSystemDataMapper
from .file_system_task_journal_record import FileSystemTaskJournalRecord
from .task import Task
from .task_events import TaskEvent


@dataclasses.dataclass
class FileSystemTaskJournal:

    records: List[FileSystemTaskJournalRecord]

    def rehydrate_task(self, task_id: str) -> Task:
        return Task.rehydrate(id=task_id, events=self.events)

    @property
    def events(self) -> Generator[TaskEvent]:
        for record in self.records:
            for event in record.events:
                yield event

    @classmethod
    def read(cls, file: BinaryIO, repair: bool = False) -> "FileSystemTaskJournal":
        records: List[FileSystemTaskJournalRecord] = []
        while True:
            pos = file.tell()
            line = file.readline()
            if not line:
                break
            try:
                record = cls.decode_record(line)
            except json.decoder.JSONDecodeError:
                if repair:
                    file.seek(pos)
                    file.truncate()
                break
            records.append(record)
        assert records
        return cls(records=records)

    @staticmethod
    def write_record(record: FileSystemTaskJournalRecord, file: BinaryIO) -> None:
        assert record.events
        data = FileSystemTaskJournal.encode_record(record)
        file.write(data + b"\n")
        file.flush()
        os.fsync(file.fileno())

    @staticmethod
    def decode_record(data: bytes) -> FileSystemTaskJournalRecord:
        dto = json.loads(data)
        return FileSystemDataMapper.load_task_journal_record(dto)

    @staticmethod
    def encode_record(record: FileSystemTaskJournalRecord) -> bytes:
        dto = FileSystemDataMapper.dump_task_journal_record(record)
        return json.dumps(dto).encode()
