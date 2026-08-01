CREATE TABLE queues (
    name TEXT PRIMARY KEY,
    dead_letter_queue TEXT REFERENCES queues(name),
    visibility_timeout REAL NOT NULL,
    created_at REAL NOT NULL
);

CREATE TABLE messages (
    id TEXT PRIMARY KEY,
    queue TEXT NOT NULL REFERENCES queues(name),
    payload BLOB NOT NULL,
    created_at REAL NOT NULL,
    visible_at REAL NOT NULL,
    version INTEGER NOT NULL
);

CREATE INDEX idx_messages_queue_created ON messages(queue, created_at);
CREATE INDEX idx_messages_visible_at ON messages(visible_at);
