# Schlange Architecture

Decisions, not rationale. Build order is in [ROADMAP.md](ROADMAP.md).

## Services

- **tasks** — task lifecycle, owns outbox publisher. Leader-elected.
- **execution** — consumes from broker, executes, reports back. Scales horizontally.
- **schedules** — fires schedules by creating tasks. Leader-elected.
- **messaging** — durable queue, session-based consumer death detection, periodic sweeper. Built-in; replaceable by external broker.
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

Contracts are defined in `schlange/api/<service>/`. The service's adapter (e.g. `services/leases/api/server.py`) satisfies them structurally; it does not import or inherit them. Contracts are written lazily — only when there's actual cross-service consumption to drive them, not speculatively for symmetry.

## Data

Each service owns its own SQLite DB. No cross-service database access. Multi-process = multiple processes opening the same DB files (WAL + busy_timeout mandatory).

## Concurrency

Threads-die-process-dies. `threading.excepthook` logs the traceback, then calls `os._exit(1)`. No silent thread death. Leader election via leases for singleton roles.

## Reliability

At-least-once everywhere. Handlers must be idempotent. No fencing tokens. No distributed transactions; the outbox pattern prevents orphans (tasks writes task row + outbox row in one transaction; a worker publishes outbox rows to the broker and marks them sent).

## Broker

RPC-style Protocol (7 RPCs): publish, claim, ack, nack, create_session, heartbeat, close_session. Request/response dataclasses, no callbacks or context managers. Direct exchange: messages carry `routing_key` (publisher-set), sessions carry `queue` (consumer subscription). Broker matches them. Competing consumers: atomic claim via `UPDATE...JOIN...RETURNING`. Dead-letter is a boolean flag on messages, not a routing key change. Nack sets flag, releases claim. No backoff or retry limit in broker — consumer-side concern.

Push-based delivery (subscribe with handler) is NOT on the Protocol. Consumer-side driving adapter (Phase 4) orchestrates RPCs into a delivery loop. Consuming service core defines a driving port; execution adapter bridges.

Protocol is internal to our SQLite broker. External brokers implement the executing service's port, not this Protocol. The port is the seam for "bring your own broker."

Consumer death via session heartbeats + periodic sweeper. Publish uses `synchronous=FULL` (durable — outbox cannot protect cross-DB). Consume (claim, ack, nack) uses `synchronous=NORMAL` (at-least-once redelivery is the contract).

## Lease

Etcd-compatible API: `acquire(key, holder, ttl)`, `refresh(key, holder)`, `release(key, holder)`, `is_holder(key, holder)`. Implementable on SQLite, etcd, Redis.

Lease holder pattern: generic background worker drives a lease via a 3-method interface (acquire/renew/release). Services needing election implement this on their core (delegating to leases). Work drivers are lease-unaware — they call a work method on the core; the core no-ops (returns early) if not leader.

Leases accepted as SPOF (k8s analogy).

## Imports

Go-style. Cross-package: `from parent import package`, then `package.Name`. Relative within a package. No aliases unless real conflict. `_base` suffix forbidden. Every package's `__init__.py` re-exports its surface.

## Out of scope

Fencing tokens, lease state caching, speculative contract packages, backwards compatibility.
