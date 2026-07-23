# Schlange Roadmap

Build order. Architecture in [ARCHITECTURE.md](ARCHITECTURE.md).

## Phase 1 — Restructure (DONE)

Monolith refactored into services layout, Go-style imports.

## Phase 2 — Leases (DONE)

Foundation: broker sweeper and outbox publisher both need leader election.

Leases service (own DB, etcd-compatible API). Tested in isolation.

## Phase 3 — Messaging (DONE)

Depends on: Phase 2.

Messaging service (own DB), RPC-style Protocol (publish, claim, ack, nack, session lifecycle), competing consumers, dead-letter boolean, session-based consumer death detection, leader-elected sweeper. Tested in isolation.

## Phase 4 — Integration

Depends on: Phases 2, 3.

tasks public contract, generic lease worker, lease holder interface, refactored core (lease holder + outbox publish), outbox worker (lease-unaware), dispatch driving adapter (subscribe loop + driving port on dispatch core), rewired composition root.

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

Sagas, external broker, multi-node — far future.

## Rules

One phase at a time. Tests for new components. No backwards compat. No speculative abstractions. Document idempotency. Per phase: design → implement → verify → commit.
