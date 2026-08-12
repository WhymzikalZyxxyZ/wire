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

**Chosen option: Option B.** Testcontainers-backed Redpanda for real broker behavior; a stubbed LEDGER HTTP boundary for deterministic, fast contract tests. `TransactionEventFlowIntegrationTest` implements four of these:

- **Happy path:** a successful LEDGER response is acknowledged and never reaches the DLQ.
- **Poison message routing:** an event LEDGER's stub rejects with a 4xx lands on the DLQ immediately, with exactly one call to LEDGER — no retry.
- **Transient retry:** an event where the stub returns 503 once and then 201 is retried and succeeds, without ever reaching the DLQ.
- **Retry exhaustion:** an event where the stub always returns 503 makes exactly 4 calls (1 initial + 3 retries) before landing on the DLQ with an "exhausted retries" reason.

Two originally-scoped scenarios were **not** built as separate tests, and that gap is named here rather than quietly dropped:

- **True ordering under concurrent multi-partition consumption** — asserting that events sharing a partition key are processed in production order requires either a single-partition, single-consumer setup (trivial, and not a meaningful test of anything) or genuine concurrent multi-partition load with an assertion on relative event ordering, which needs more test infrastructure than this pass built. WIRE's ordering guarantee rests on Kafka/Redpanda's own documented per-partition ordering behavior, exercised structurally by production topology rather than independently re-verified here.
- **True crash-then-redelivery** — simulating an actual consumer crash mid-processing (killing the JVM after a LEDGER call succeeds but before the offset commits) isn't something a single-process JUnit test can force cleanly. What *is* tested is the piece that actually matters for correctness: a redelivered call reaching LEDGER with the same `idempotencyKey` is safe, because LEDGER's own `TransactionServiceIntegrationTest.resubmittingTheSameIdempotencyKeyReturnsTheOriginalResultWithoutDoublePosting` already proves that. WIRE's own tests don't re-simulate the crash; they rely on (and cite) that already-proven guarantee, same as ADR-002 intended.

## Consequences

**Positive:**
- The four failure modes ADR-003 actually names — success, terminal rejection, transient recovery, retry exhaustion — each have a test that would fail if the behavior broke, not just a design document asserting it
- Fast, self-contained CI — no dependency on a second repository's application being built, deployed, or reachable

**Negative / accepted tradeoffs:**
- The LEDGER-contract stub needs deliberate upkeep as LEDGER's real API evolves — accepted as a real coupling cost of testing at a service boundary, named explicitly rather than hidden
- Ordering and crash-redelivery, as described above, are proven at the boundary of "what Kafka/Redpanda and LEDGER already guarantee," not independently re-verified by WIRE's own suite — a narrower claim than this ADR originally scoped, corrected here rather than left overstated

**Risks:**
- None outstanding for the four scenarios actually tested — they exist, run in CI, and pass. The two narrowed-scope items above are the accepted, named gap; see `docs/RISKS.md`.

## Notes

Same posture as LEDGER's ADR-004 at the same stage — this was the ADR whose promise wasn't fulfilled yet at design time. The implementation pass corrected the original test list to match what a single-process test suite can actually prove cleanly, rather than writing brittle tests to hit an artificial scope target.
