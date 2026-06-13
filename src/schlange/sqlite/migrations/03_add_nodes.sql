CREATE TABLE nodes (
    id TEXT PRIMARY KEY,
    version INTEGER NOT NULL,
    last_heartbeat_at TEXT NOT NULL
);
