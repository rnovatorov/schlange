CREATE TABLE tasks (
    id TEXT PRIMARY KEY,
    version INTEGER NOT NULL,
    created_at REAL NOT NULL,
    args TEXT NOT NULL,
    state TEXT NOT NULL,
    ready_at REAL NOT NULL,
    retry_policy TEXT NOT NULL,
    executions TEXT NOT NULL,
    last_execution_ended_at REAL,
    execution_in_progress INTEGER NOT NULL,
    schedule_id TEXT,
    kind TEXT NOT NULL
);

CREATE UNIQUE INDEX idx_schedule_id_where_active ON tasks (schedule_id)
WHERE
    state = 'ACTIVE';

CREATE INDEX idx_ready_at_where_active ON tasks (ready_at)
WHERE
    state = 'ACTIVE';

CREATE INDEX idx_last_execution_ended_at_where_succeeded_or_failed ON tasks
    (last_execution_ended_at)
WHERE
    state = 'SUCCEEDED' OR state = 'FAILED';
