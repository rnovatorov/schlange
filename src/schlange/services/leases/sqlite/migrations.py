from schlange.internal.sqlite import Migration

MIGRATIONS: list[Migration] = [
    Migration(
        statements=[
            """
            CREATE TABLE leases (
                key TEXT PRIMARY KEY,
                holder TEXT NOT NULL,
                ttl REAL NOT NULL,
                expires_at REAL NOT NULL
            )
            """,
        ]
    ),
]
