# Schlange Roadmap

Build order. Architecture in [ARCHITECTURE.md](ARCHITECTURE.md).

## Phase 1 — Restructure (DONE)

Monolith refactored into services layout, Go-style imports.

## Phase 2 — Leases (DONE)

Foundation: broker sweeper and outbox publisher both need leader election.

Leases service (own DB, etcd-compatible API). Tested in isolation.

## Phase 3 — Messaging (DONE)

Depends on: Phase 2.

Messaging service (own DB), SQS-like RPC Protocol (declare_queue, publish, claim, ack, requeue), competing consumers, per-message visibility timeout, per-queue DLQ with max_delivery_count. Tested in isolation.

Deviation from plan: sessions and heartbeat-based consumer death detection were built, then replaced by the simpler SQS model — visibility-timeout expiry covers consumer death, so no sessions, heartbeats, or sweeper.

## Phase 4 — Integration (DONE)

Depends on: Phases 2, 3.

Tasks public contract (`kind` on tasks, `end_execution` by execution seq_num), Dispatcher (acquire-on-tick leader election, begins executions, publishes to broker, one outstanding at a time), execution service (stateless, handler registry + tasks port), per-kind consumer workers, rewired composition root.

Deviation from plan: the tasks Sweeper was dropped — broker visibility-timeout redelivery recovers crashed executors, making a sweeper unnecessary.

Milestone: create-task-then-execute works through the new architecture. ✓ (see examples/)

## Phase 5 — Schedules migration (PARTIAL)

Depends on: Phase 4.

Schedules refactored to the same pattern (api/core/sqlite/background). Schedule firing consumes tasks through the public contract via a driving adapter (`services/schedules/api/task_service.py`), not tasks core directly. Firing is lease-gated — the ScheduleWorker acquires the `schedules-worker` lease each tick, mirroring the Dispatcher. Deterministic task ids still cover crash recovery (leader dies mid-fire, lease expires, new leader re-fires the same sequence → no-op).

Done: api/core/sqlite/background structure; schedules consumes tasks via the tasks API driving adapter; lease-gated firing.

Remaining: public contract in `schlange/api/schedules/` (deferred — no consumer yet, per the lazy-contracts rule).

Milestone: schedule demo works through the new path.

## Phase 6 — Multi-process validation

Depends on: Phases 2-5.

N processes on one machine. Validate leader election, distribution, recovery. Document deployment. Benchmark SQLite contention.

Milestone: N-process single-node works. Performance characterized.

## Beyond

Sagas, external broker, multi-node — far future.

## Rules

One phase at a time. Tests for new components. No backwards compat. No speculative abstractions. Document idempotency. Per phase: design → implement → verify → commit.
