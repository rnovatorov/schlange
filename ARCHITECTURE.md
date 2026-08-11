# Schlange Architecture

Decisions, not rationale. Build order is in [ROADMAP.md](ROADMAP.md).

## Services

- **tasks** — task lifecycle, owns Dispatcher (publishes executable tasks to broker). Tasks carry a `kind` for routing. Dispatcher is leader-elected.
- **execution** — stateless. Consumes from broker, runs handlers, reports back via `end_execution`. Scales horizontally (per-kind consumer workers).
- **schedules** — fires schedules by creating tasks. Not leader-elected: firing is idempotent via deterministic task ids.
- **messaging** — durable SQS-like queue: visibility-timeout redelivery, per-queue DLQ. Built-in; replaceable by external broker.
- **leases** — leader election primitive.

## Service structure

Thin API adapter (exposes public contract) wrapping stateless core (business logic, ports declared as Protocols). Adapter takes only core. Persistence and workers private to the service.

Implicit interface implementation (structural typing, no Protocol inheritance).

Execution is the exception: it has no persistence — core service plus consumer workers.

## Package layout

```
src/schlange/
├── api/<service>/       # public contracts (Protocols, request/response dataclasses, errors)
├── services/<service>/
│   ├── api/             # thin adapter, wraps core, satisfies Protocols from schlange/api/
│   ├── core/            # stateless business logic
│   ├── sqlite/          # persistence (private to service; absent in execution)
│   └── background/      # workers (private to service)
├── internal/
│   ├── background/      # Worker base
│   ├── core/            # shared primitives (Aggregate, DTO, RetryPolicy, TooManyAttemptsError)
│   └── sqlite/          # shared SQLite plumbing (connection, transaction, DataMapper base)
└── cli/
```

Contracts are defined in `schlange/api/<service>/` (currently: tasks, messaging, leases). The service's adapter (e.g. `services/leases/api/server.py`) satisfies them structurally; it does not import or inherit them. Contracts are written lazily — only when there's actual cross-service consumption to drive them, not speculatively for symmetry.

Cross-service consumption goes through driving adapters on the consumer side: tasks adapts messaging (`services/tasks/api/message_queue.py`) and leases (`services/tasks/api/lease_service.py`) to its core ports; execution adapts tasks (`services/execution/api/task_service.py`). Driving adapters import only the public contract — the errors a Server raises are part of that contract (`api/<service>/errors.py`), not the provider's private package.

## Data

Each stateful service owns its own SQLite DB (tasks, schedules, messaging, leases). Execution has no DB. No cross-service database access. Multi-process = multiple processes opening the same DB files (WAL mandatory, foreign keys enforced).

Each DB opens three connection pools: read (`synchronous=NORMAL`), write (`synchronous=NORMAL`), sync write (`synchronous=FULL`). Callers pick the pool per transaction.

## Concurrency

Crash propagation: a worker thread that raises stores the error and sends SIGINT to its own process; `wait()` re-raises the stored error. No silent thread death. `Schlange.stop()` cancels all workers, then raises `ExceptionGroup` if any failed. Leader election via leases for singleton roles (Dispatcher).

## Reliability

At-least-once everywhere. Handlers must be idempotent. No fencing tokens. No distributed transactions.

Task dispatch: the Dispatcher begins an execution, publishes to the broker, and only then commits the task (publish-before-commit). A crash between publish and commit causes redispatch and a duplicate execution — never a task stuck with an execution begun but no message. One outstanding execution per task, enforced by a domain guard (`TaskExecutionNotEndedYetError`) and an `execution_in_progress` query filter. `end_execution` is idempotent by execution seq_num; duplicate calls from redelivery are no-ops. Publish commits with `synchronous=FULL` (durable — outbox cannot protect cross-DB).

Writes default to `synchronous=FULL`. Hot paths explicitly downgrade to `synchronous=NORMAL`: broker claim/ack/requeue, the begin_execution task commit, schedule firing. A lost NORMAL commit means redelivery and re-execution — at-least-once is the contract.

Executor crashes are recovered by the broker: the claimed message's visibility timeout expires, the message is redelivered, the handler re-runs, and `end_execution` no-ops if the execution already ended. No sweeper needed.

Task retries are a tasks-service concern: exponential backoff via `RetryPolicy`, attempts exhausted → task FAILED. Broker redelivery is separate: per-queue `max_delivery_count`, then DLQ. The two limits are independent; either can fire first.

## Broker

SQS-like. RPC-style Protocol (5 RPCs): `declare_queue`, `publish_message`, `claim_message`, `ack_message`, `requeue_message`. Request/response dataclasses, no callbacks or context managers. One queue per task kind; consumers subscribe by queue name. Competing consumers: atomic claim via `UPDATE ... RETURNING` (bumps `visible_at`, `delivery_count`, `version`). Per-message visibility timeout, set by the publisher. Ack and requeue are version-checked (optimistic concurrency).

DLQ is a separate queue, not a message flag: `requeue_message` moves the message to the queue's DLQ once `delivery_count` reaches `max_delivery_count` (deletes it if the queue has no DLQ). The tasks adapter declares queues lazily on first publish per kind: `{kind}` with DLQ `{kind}.dlq`.

No sessions, no heartbeats, no sweeper: consumer death is detected by visibility-timeout expiry.

Protocol is internal to our SQLite broker. External brokers implement the consuming service's port, not this Protocol. The port is the seam for "bring your own broker."

## Lease

Etcd-compatible API: `acquire(key, holder, ttl)`, `refresh(key, holder)`, `release(key, holder)`, `is_holder(key, holder)`. Implementable on SQLite, etcd, Redis.

Lease holder pattern: each leader-gated worker acquires its lease every tick (acquire-or-renew is idempotent). If acquired, do work; if not, skip. TTL must exceed worker interval so the lease survives between ticks. Let expire on death. Per-worker lease keys (e.g. `tasks-dispatcher`). Workers use only `acquire` — one call per tick covers both leadership check and renewal; no refresher thread. A background Reaper deletes expired leases.

Leases accepted as SPOF (k8s analogy).

## Imports

Go-style. Cross-package: `from parent import package`, then `package.Name`. Relative within a package. No aliases unless real conflict. `_base` suffix forbidden. Every package's `__init__.py` re-exports its surface.

## Out of scope

Fencing tokens, lease state caching, speculative contract packages, backwards compatibility.
