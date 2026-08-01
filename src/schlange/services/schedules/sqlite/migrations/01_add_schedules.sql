CREATE TABLE schedules (
    id TEXT PRIMARY KEY,
    version INT NOT NULL,
    created_at REAL NOT NULL,
    ready_at REAL NOT NULL,
    origin REAL NOT NULL,
    interval REAL NOT NULL,
    retry_policy TEXT NOT NULL,
    enabled INT NOT NULL,
    task_args TEXT NOT NULL,
    task_retry_policy TEXT NOT NULL,
    task_sequence_number INT NOT NULL,
    firings TEXT NOT NULL,
    task_kind TEXT NOT NULL
);

CREATE INDEX idx_ready_at_where_enabled ON schedules (ready_at)
WHERE
    enabled = 1;
