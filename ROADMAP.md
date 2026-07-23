# Schlange Roadmap

Build order. Architecture in [ARCHITECTURE.md](ARCHITECTURE.md).

## Phase 1 — Restructure (DONE)

Monolith refactored into services layout, Go-style imports.

## Phase 2 — Leases

Foundation: broker sweeper and outbox publisher both need leader election.

Leases service (own DB, etcd-compatible API), generic lease worker, lease holder interface. Tested in isolation.

Open: TTL semantics, reaper strategy (query-time vs background), test approach.

## Phase 3 — Messaging

Depends on: Phase 2.

Broker service (own DB), publish on protocol, subscribe via context manager (concrete-impl-only), session-based consumer death detection, leader-elected sweeper. Tested in isolation, no integration yet.

Open: schema details, sweeper cadence, session lifecycle.

## Phase 4 — Integration

Depends on: Phases 2, 3.

tasks public contract, refactored core (lease holder + outbox publish), outbox worker (lease-unaware), dispatch consuming from broker, rewired composition root.

Milestone: create-task-then-execute works through the new architecture.

## Phase 5 — Schedules migration

Depends on: Phase 4.

Schedules refactored to the same pattern (api/core/sqlite/background), leader-elected firing.

Milestone: schedule demo works through the new path.

## Phase 6 — Multi-process validation

Depends on: Phases 2-5.

N processes on one machine. Validate leader election, distribution, recovery. Document deployment. Benchmark SQLite contention.

Milestone: N-process single-node works. Performance characterized.

## Beyond

Not planned. External broker, multi-node — far future.

## Rules

One phase at a time. Tests for new components. No backwards compat. No speculative abstractions. Document idempotency. Per phase: design → implement → verify → commit.
