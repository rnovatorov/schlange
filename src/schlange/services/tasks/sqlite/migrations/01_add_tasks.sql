CREATE TABLE tasks (
    id TEXT PRIMARY KEY,
    version INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    args TEXT NOT NULL,
    state TEXT NOT NULL,
    ready_at TEXT NOT NULL,
    retry_policy TEXT NOT NULL,
    executions TEXT NOT NULL,
    last_execution_ended_at TEXT,
    schedule_id TEXT
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
