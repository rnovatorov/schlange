# Schlange Architecture

Decisions, not rationale. Build order is in [ROADMAP.md](ROADMAP.md).

## Services

- **tasks** — task lifecycle, owns outbox publisher. Leader-elected.
- **dispatch** — consumes from broker, executes, reports back. Scales horizontally.
- **schedules** — fires schedules by creating tasks. Leader-elected.
- **messaging** — durable queue, session-based consumer death detection, leader-elected sweeper. Built-in; replaceable by external broker.
- **leases** — leader election primitive.

## Service structure

Thin API adapter (exposes public contract) wrapping stateless core (business logic + lease delegation if leader-elected). Adapter takes only core. Persistence and workers private to the service.

Implicit interface implementation (structural typing, no Protocol inheritance).

## Package layout

```
src/schlange/
├── api/<service>/       # public contracts (Protocols + dataclasses); created lazily, not speculatively
├── services/<service>/
│   ├── api/             # thin adapter, wraps core, satisfies Protocols from schlange/api/
│   ├── core/            # stateless business logic
│   ├── sqlite/          # persistence (private to service)
│   └── background/      # workers (private to service)
├── internal/
│   ├── background/      # Worker base, generic LeaseWorker, LeaseHolder protocol
│   ├── core/            # shared primitives (Aggregate, DTO, RetryPolicy, TooManyAttemptsError)
│   └── sqlite/          # shared SQLite plumbing (connection, transaction, DataMapper base)
└── cli/
```

Contracts are defined in `schlange/api/<service>/`. The service's adapter (e.g. `services/leases/api/lease_server.py`) satisfies them structurally; it does not import or inherit them. Contracts are written lazily — only when there's actual cross-service consumption to drive them, not speculatively for symmetry.

## Data

Each service owns its own SQLite DB. No cross-service database access. Multi-process = multiple processes opening the same DB files (WAL + busy_timeout mandatory).

## Concurrency

Threads-die-process-dies. `threading.excepthook` logs the traceback, then calls `os._exit(1)`. No silent thread death. Leader election via leases for singleton roles.

## Reliability

At-least-once everywhere. Handlers must be idempotent. No fencing tokens. No distributed transactions; the outbox pattern prevents orphans (tasks writes task row + outbox row in one transaction; a worker publishes outbox rows to the broker and marks them sent).

## Broker

One impl, multi-process capable. Protocol: `publish(payload: bytes) -> str` only. Subscribe is concrete-impl-only, exposed as a context manager — `subscribe(handler: Callable[[Message], None])`; the broker delivers messages to the handler while the context is active, stops on exit, waits for in-flight handler to complete. Handler contract: raise = nack, return = ack, one message at a time. Consumer death via session heartbeats + leader-elected sweeper.

## Lease

Etcd-compatible API: `acquire(key, holder, ttl)`, `refresh(key, holder)`, `release(key, holder)`, `is_holder(key, holder)`. Implementable on SQLite, etcd, Redis.

Lease holder pattern: generic background worker drives a lease via a 3-method interface (acquire/renew/release). Services needing election implement this on their core (delegating to leases). Work drivers are lease-unaware — they call a work method on the core; the core no-ops (returns early) if not leader.

Leases accepted as SPOF (k8s analogy).

## Imports

Go-style. Cross-package: `from parent import package`, then `package.Name`. Relative within a package. No aliases unless real conflict. `_base` suffix forbidden. Every package's `__init__.py` re-exports its surface.

## Out of scope

Fencing tokens, lease state caching, speculative contract packages, backwards compatibility.
