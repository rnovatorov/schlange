# Schlange Microservices Architecture — Handoff Document

## Project Context

**Schlange** is a hobby Python project at `/home/rnovatorov/code/rnovatorov/schlange`. Currently a lightweight, persistent, single-node task queue and scheduler on SQLite. ~2,673 LOC. Markets itself as the simple alternative to RabbitMQ/Redis/Kafka. Published on PyPI as `schlange-queue` (no real users).

The user is **refactoring it into a microservices architecture**. Motivation: enjoyment of building distributed systems, not shipping. *"I don't care when it ships. I just want to build it because I like building distributed systems."*

The user previously had SPEC-001/002/003 (multi-process support baked into the Task aggregate via `claimed_by`, `Node` entities, heartbeats). **They reverted SPEC-001** because it mixed concerns — distributed-systems machinery leaked into the Task domain model. The new design fixes this by moving distributed-systems concerns into a dedicated MessageBroker.

## Goal

A services-oriented architecture that supports two deployment modes:

- **Single-process (monolith)**: zero dependencies, in-process broker backed by SQLite. Preserves current UX.
- **Multi-process / multi-node**: scale executors across machines; operator brings a real broker (RabbitMQ, Redis Streams, etc.).

## Architectural Decisions (Settled)

### Services

Five services, named as `[Domain][Role]`:

- **TaskManager** — task state CRUD, outbox publishing
- **TaskExecutor** — consumes from broker, runs task handlers, reports results to TaskManager
- **ScheduleManager** — schedule state, fires schedules by creating tasks
- **MessageBroker** — message queue (interface + impls)
- **LeaseManager** — leader election for singleton roles

### Package Layout

```
src/schlange/
├── api/                        # public contracts (Protocols + dataclasses)
│   ├── task_manager/           # protocol.py, messages.py, errors.py
│   ├── task_executor/
│   ├── message_broker/
│   ├── lease_manager/
│   └── schedule_manager/
├── services/                   # implementations
│   ├── task_manager/
│   │   ├── api/                # concrete class implementing the Protocol
│   │   ├── core/               # private domain logic
│   │   ├── sqlite/             # private persistence
│   │   └── background/         # private workers (outbox publisher, cleanup)
│   ├── task_executor/
│   ├── schedule_manager/
│   ├── message_broker/
│   │   ├── api/
│   │   ├── core/
│   │   ├── sqlite/             # Tier 1 (in-process) & Tier 2 (multi-process) impls
│   │   └── background/         # sweeper for crashed consumers
│   └── lease_manager/
├── internal/                   # shared plumbing (Go-style internal/)
│   ├── sqlite/                 # connection, pool, transaction
│   ├── background/             # Worker base class
│   └── ddd/                    # Aggregate, DTO, Specification primitives
└── cli/                        # CLI entry points, composition root
```

**Layering rules:**

- `api/` = pure Protocols + dataclasses only. No business logic, no helpers, no I/O. This is the only thing consumers depend on.
- `services/<name>/core/` = private domain. Not imported outside the service.
- `services/<name>/api/` = concrete class implementing the Protocol from `api/<name>.py`.
- `internal/` = shared plumbing within schlange (Go `internal/` semantics — importable by schlange code, not by end users).

### Broker Interface (Settled)

```python
class BrokerProtocol(Protocol):
    def publish(self, payload: bytes) -> str: ...

@dataclass
class Message:
    id: str
    payload: bytes
```

**Subscription is NOT on the Protocol.** It's impl-specific (in-process queue vs AMQP channel vs Redis consumer group). Concrete broker impls expose subscription via a context manager:

```python
class InProcessBroker:  # implements BrokerProtocol
    def publish(self, payload: bytes) -> str: ...

    @contextlib.contextmanager
    def subscribe(self, handler: Callable[[Message], None]) -> Iterator[None]:
        # spawn delivery thread, start consuming
        yield
        # stop delivery, wait for in-flight, cleanup
```

**Handler contract:**

- Returns normally → ack (message removed permanently)
- Raises → nack (message redelivered)
- One message at a time (no prefetch, natural backpressure)

### Task Lifecycle

1. Client → `TaskManager.submit_task(id, kind, args)` — writes task to DB, returns. Dedup on `id`.
2. **OutboxPublisherWorker** (internal to TaskManager, singleton via leader election) — polls DB for ready tasks, calls `broker.publish(payload)`. After publish, marks task as enqueued. If mark fails, message may be duplicated — accepted.
3. Broker delivers to `TaskExecutor.handle_message(message)`.
4. TaskExecutor deserializes payload, looks up handler by kind, runs it.
5. TaskExecutor reports result to TaskManager (`report_execution`).
6. Handler returns normally → broker acks; handler raises → broker nacks (redelivers).

### Reliability Model

- **At-least-once delivery, everywhere, always.** This includes single-process mode — the SQLite broker is durable.
- This is a deliberate contrast with Celery, whose filesystem/memory broker fakes have at-most-once semantics (messages lost if the process dies). Schlange will never have at-most-once.
- **Idempotent handlers required** (document loudly).
- **Outbox pattern** for task publish — no orphans, no distributed transactions.
- **No distributed transactions ever.** Sagas maybe later.
- **Worker-side execution timeout** for hung handlers. Use processes (not threads) for handlers in CPython — threads can't be reliably killed.
- **Heartbeat-based consumer death detection** (in shared-SQLite broker): consumer writes heartbeat to `sessions` table; background sweeper requeues messages held by stale sessions.
- **"Never lose messages by default"** — claim is achievable given the above, but depends on SQLite durability, correct visibility/heartbeat tuning, and mark-done-before-ack ordering.

### Multi-Process on Single Node

The user wants N schlange processes on one machine to coexist gracefully. Implications:

- **LeaseManager is needed for multi-process, not just multi-node.** Even "I started it twice" on one node triggers leader election needs.
- **Shared-SQLite broker** (Tier 2) for multi-process single-node. Same pattern as the reverted SPEC-001, but at the broker layer (where it belongs) instead of the task domain.
- **Singleton roles** (OutboxPublisher, ScheduleFiring) are leader-elected.

### SQLite Broker Requirements

For multi-process shared-SQLite:

- `PRAGMA journal_mode=WAL` mandatory on every connection.
- `busy_timeout` 500ms–5s mandatory (default is 0 = instant failure).
- Retry-on-`SQLITE_BUSY` mandatory even with `busy_timeout`.
- No NFS, ever — document loudly.
- Throughput ceiling: low-thousands of write-ops/sec total across all processes (every publish, consume, ack, claim, heartbeat is a write).
- Benchmark before assuming it's enough.

### Microservices Deployment

When going multi-node: operator deploys a real broker (RabbitMQ/Redis/SQS). Broker impl is swapped. README pitch changes to "single-node: zero dependencies; multi-node: bring your own broker."

The shape (producer → broker → worker, with separate scheduler) is superficially similar to Celery's, but the **semantics differ**: Celery's in-process/filesystem broker fakes have at-most-once delivery (messages lost if the process dies). Schlange's design is **at-least-once everywhere, including single-process mode**, because the broker is durable (SQLite-backed) and the outbox pattern prevents orphan tasks.

## Build Order (Working-Product-First)

The user explicitly rejected "interest-density" ordering. The right metric is **smallest end-to-end runnable thing first, then add pieces while always having something that runs.**

1. **Restructure layout** — add `api/`, `services/`, keep `internal/`. Don't refactor logic yet.
2. **`BrokerProtocol` + `InProcessBroker` (Tier 1)** — trivial queue table, no heartbeats, no sweeper, single-process.
3. **TaskManager component** — wraps existing `core.TaskService`, adds `OutboxPublisherWorker`.
4. **TaskExecutor component** — wraps existing `ExecutionWorker`, consumes from broker via `handle_message`.
5. **Wire monolith** — first end-to-end runnable system. Submit task, watch it execute.
6. **ScheduleManager component** — moves existing `schedule_service` + `schedule_worker` into component shape.
7. **CleanupWorker** moves internal to TaskManager.
8. **LeaseManager + leader election** on OutboxPublisher and ScheduleFiring.
9. **Tier-2 SQLite broker impl** (multi-process single-node).
10. **Tier-3 broker impl** (RabbitMQ/Redis client — multi-node).

## Conversational Style — Critical

The user wants **rigorous architectural pushback**. Their original prompt was *"interview me relentlessly."* They engage as a peer, not a student.

**Patterns that work:**

- One question at a time. They explicitly asked for this. Don't dump 3-question batteries.
- Concrete code sketches (Python) — they engage well with code.
- Sharp trade-off framing — option A vs B vs C with consequences.
- Steelman their proposals before critiquing.
- Acknowledge evidence when it updates your view; don't dogpile.
- Terse exchanges beat long monologues. Match their pace.

**Patterns to AVOID (my recent errors):**

- **Over-engineering.** I repeatedly added abstractions the user didn't want (ack/nack methods on Message, Subscription as separate concept, start/stop on Protocol). The user is more minimalist than I was acting. **They have been fixing my over-engineering more than the reverse.**
- **Hallucinating lifecycle ownership.** I had `TaskExecutor` "starting" the broker — wrong. Services don't manage infrastructure lifecycle. Composition root does.
- **Adding methods to Protocols that belong on concrete impls.** Protocols should be minimal — only the polymorphic surface. Lifecycle/subscription/etc. is impl-specific.
- **Re-asking questions the user already answered.** I did this with handler exception semantics after they'd already said "raise = nack, return = ack."
- **Long monologues when they're moving fast.** They get curt when responses are too long (*"Please one question at a time"*).
- **Loose comparisons to other systems** (e.g., "this is just Celery") without verifying the comparison is accurate. The user knows the landscape well and will catch sloppy analogies.

## Open Questions (Next Agent's Agenda)

Likely next topic: **`TaskManagerProtocol` interface design.** TaskExecutor depends on it for `report_execution`. What other methods? `submit_task`, `begin_execution`, `report_execution` — what's the contract surface?

Other open:

- `LeaseManager` interface details (leader election mechanism on SQLite).
- `ScheduleManager` interface (probably similar shape to TaskManager).
- Schema migration strategy across multi-process rolling deploys.
- Concrete `InProcessBroker` impl details — `sessions` and `messages` table schemas.
- CLI as composition root — concrete structure.
- Library API for programmatic monolith composition (user said *"we'll need to provide an easy way to build a working monolith"*).

## Useful Project Facts

- Tests: `pipenv run python -m unittest discover -v` (currently only 1 test passes after the SPEC-001 revert).
- Lint: `make lint` runs black, isort, mypy, pyflakes.
- Public API today: `with Schlange.new() as sch: sch.create_task(...)`. Backwards compat is explicitly NOT a concern (*"no one uses the project"*).
- The user is open to dropping the existing public API if it serves the new architecture.
- Specs directory contains `SPEC-002-task-claiming.md` and `SPEC-003-crash-recovery.md` (untracked, from the abandoned SPEC-001 trajectory — these may inform the new design but are not directly adopted).
