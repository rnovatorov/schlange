CREATE TABLE queues (
    name TEXT PRIMARY KEY,
    dead_letter_queue TEXT,
    max_delivery_count INTEGER NOT NULL,
    created_at REAL NOT NULL,
    FOREIGN KEY (dead_letter_queue) REFERENCES queues(name)
);

CREATE TABLE messages (
    id TEXT PRIMARY KEY,
    queue TEXT NOT NULL,
    payload BLOB NOT NULL,
    visibility_timeout REAL NOT NULL,
    delivery_count INTEGER NOT NULL,
    visible_at REAL NOT NULL,
    created_at REAL NOT NULL,
    version INTEGER NOT NULL,
    FOREIGN KEY (queue) REFERENCES queues(name) ON DELETE CASCADE
);

CREATE INDEX idx_messages_queue_created ON messages(queue, created_at);
CREATE INDEX idx_messages_visible_at ON messages(visible_at);
