CREATE TABLE messages (
    id TEXT PRIMARY KEY,
    routing_key TEXT NOT NULL,
    payload BLOB NOT NULL,
    created_at TEXT NOT NULL,
    is_dead_letter INTEGER NOT NULL DEFAULT 0,
    claimed_by TEXT,
    claimed_at TEXT
);

CREATE TABLE sessions (
    id TEXT PRIMARY KEY,
    queue TEXT NOT NULL,
    dead_letter INTEGER NOT NULL DEFAULT 0,
    last_heartbeat_at TEXT NOT NULL,
    created_at TEXT NOT NULL
);
