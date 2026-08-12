# ADR-004: Correctness Verification Strategy

**Date:** 2026-08-12
**Status:** Accepted
**Deciders:** Zyxxyz

---

## Context

ADR-002 and ADR-003 make specific claims: per-account ordering holds, redelivery doesn't double-post, transient failures self-heal, terminal failures land on the DLQ without blocking the partition. As with LEDGER, a design document asserting these is not the same thing as a test that would fail if any of them broke — and this repo learned that lesson concretely once already, when LEDGER's first real CI run caught a Testcontainers container-lifecycle bug that no ADR would have caught on its own.

WIRE also introduces a question LEDGER didn't have: this service's job is to call *another* service's API. Its tests need to verify WIRE's own behavior at that boundary without either mocking away the meaningful part or requiring a second whole repository's application to be running.

## Decision Drivers

- Tests need to run against a real broker (Redpanda via Testcontainers), for the same reason LEDGER's tests run against real PostgreSQL — partition/consumer-group/offset behavior is the broker's actual behavior, not something a mocked client library can stand in for
- Tests should verify WIRE's contract with LEDGER (the shape of the HTTP call, retry behavior, how `eventId` becomes `idempotencyKey`) without depending on a live LEDGER instance being reachable in CI — that would couple two repositories' pipelines together for no real benefit
- The specific failure modes named in ADR-002/003 (out-of-order processing, duplicate posting on redelivery, poison messages blocking a partition) each need a test that actually provokes them, not just a happy-path test

## Options Considered

### Option A — Unit tests only, with a mocked Kafka client and a mocked HTTP client
Fast, no infrastructure.

**Pros:** Quick to write and run.
**Cons:** A mocked consumer can't demonstrate real partition-assignment or offset-commit behavior — the exact thing ADR-002's ordering and redelivery claims depend on. Same structural blind spot LEDGER's ADR-004 already rejected for the database layer, for the same reason.

### Option B — Testcontainers Redpanda for the broker, a stub HTTP server standing in for LEDGER's API contract
Real broker, real consumer group, real partitioning and offset commits — but LEDGER itself is represented by a stub server (e.g. WireMock) programmed to return the specific responses being tested against (success, a duplicate-idempotency-key response, a validation 4xx, a transient 503), not a running instance of the LEDGER application.

**Pros:** Proves the actual thing being claimed — real Kafka-protocol partitioning, consumer-group rebalancing, and manual offset-commit behavior — while keeping WIRE's test suite self-contained and fast. The stub server lets tests deterministically provoke every failure branch in ADR-003 (transient vs. terminal) on demand, which would be much harder to force reliably against a real LEDGER instance.
**Cons:** A stub can drift from LEDGER's real API if LEDGER's contract changes without WIRE's stub being updated to match — a real risk, tracked explicitly in `docs/RISKS.md` rather than assumed away.

## Decision

**Chosen option: Option B.** Testcontainers-backed Redpanda for real broker behavior; a stubbed LEDGER HTTP boundary for deterministic, fast contract tests. The specific tests this commits to, once the consumer exists:

- **Ordering:** produce a sequence of events for the same `accountId` out of production order is not possible to fake meaningfully at the partition level — instead, assert that events sharing an `accountId` are always routed to the same partition, and that a single-partition consumer processes them in the order they were produced.
- **Redelivery / idempotency:** simulate a consumer crash after a successful LEDGER call but before offset commit (by forcing a redelivery), assert the second delivery calls LEDGER again with the same `idempotencyKey` and that WIRE treats LEDGER's "already exists" response as success, not as a new failure.
- **Poison message routing:** feed a malformed event and an event LEDGER's stub rejects with a 4xx, assert both land on the DLQ topic without blocking subsequent events on the same partition.
- **Transient retry:** feed an event where the stub returns 503 for the first N calls and success after, assert WIRE retries with backoff and eventually succeeds rather than prematurely dead-lettering it.

## Consequences

**Positive:**
- Every specific claim in ADR-002 and ADR-003 gets a test named after the exact failure mode it exists to prevent, not just a generic "happy path" test
- Fast, self-contained CI — no dependency on a second repository's application being built, deployed, or reachable

**Negative / accepted tradeoffs:**
- The LEDGER-contract stub needs deliberate upkeep as LEDGER's real API evolves — accepted as a real coupling cost of testing at a service boundary, named explicitly rather than hidden
- As with LEDGER, concurrency/timing-sensitive tests are inherently harder to keep deterministic than sequential ones — will need the same care (forced synchronization points rather than sleeps) that LEDGER's `TransactionServiceConcurrencyTest` used

**Risks:**
- **Not built yet.** This ADR documents the target verification strategy — no consumer, producer, or tests exist in this repository yet (see `docs/RISKS.md`). Until they exist and pass, ADR-002's and ADR-003's claims are design intent, not proven fact.

## Notes

Same posture as LEDGER's ADR-004 at the same stage: this is the ADR whose promise isn't fulfilled yet, and the next implementation pass should treat writing these tests as inseparable from writing the consumer itself.
