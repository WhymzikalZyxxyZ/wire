# ADR-002: Delivery & Consistency Model

**Date:** 2026-08-12
**Status:** Accepted
**Deciders:** Zyxxyz

---

## Context

A message broker gives at-least-once delivery by default (a consumer that crashes after processing a message but before committing its offset will see that message again). WIRE's job is to turn that into a system where transaction events reach LEDGER exactly-once *in effect* — no dropped events, no double-posted transactions — without lying about what the broker actually guarantees.

WIRE also needs an ordering story: LEDGER's `account_balances` locking (see LEDGER's ADR-002) already handles concurrent writes to the same account correctly, but if events for the same account can be processed wildly out of order upstream of LEDGER, WIRE would be feeding LEDGER a stream that no longer reflects the order transactions actually happened in — a correctness problem LEDGER's own locking can't see or fix, because it only knows about the requests it's actually asked to process.

## Decision Drivers

- Delivery semantics must be provably at-least-once-and-idempotent, not accidentally at-most-once (silently dropping events on failure is worse than reprocessing them)
- Per-account ordering matters even though LEDGER's own writes are safe under concurrency — an out-of-order stream is a data-quality problem, not just a locking problem
- The consistency mechanism should build on LEDGER's existing idempotency guarantee (ADR-003 in LEDGER) rather than re-inventing a second, parallel idempotency system

## Options Considered

### Option A — Auto-commit offsets, best-effort delivery to LEDGER
The simplest Kafka consumer configuration: offsets commit on a timer, independent of whether the LEDGER call succeeded.

**Pros:** Simplest possible consumer code.
**Cons:** A crash between offset commit and a successful LEDGER call silently loses that event — no redelivery, no record it ever happened. Unacceptable for financial event data; this is the failure mode the whole project exists to avoid.

### Option B — Manual offset commit after a confirmed LEDGER response, keyed for per-account ordering, idempotency delegated to LEDGER
The consumer only commits an offset after LEDGER has responded (success, or a definitively-terminal rejection like validation failure routed to the DLQ — see [ADR-003](003-failure-handling.md)). Every event carries a stable `eventId`, mapped 1:1 to LEDGER's `idempotencyKey`, so a redelivered event (from a crash-before-commit) posts to LEDGER a second time and gets LEDGER's existing "already exists, here's the original result" response instead of a duplicate transaction. Producers key each event by `accountId`, so Kafka's partitioning guarantees every event for a given account lands in the same partition and is processed by the same consumer in the order it was produced.

**Pros:** No event is ever acknowledged to the broker until LEDGER has durably recorded it (or definitively rejected it) — a crash anywhere in the pipeline causes redelivery, never silent loss. Idempotency piggybacks on a mechanism that's already proven correct (LEDGER's DB-level `UNIQUE` constraint on `idempotency_key`) instead of building a second one in WIRE that could disagree with the first. Per-account ordering is a property of the partitioning scheme, not application-level bookkeeping.
**Cons:** Slower per-partition throughput than auto-commit (each event effectively waits for a round trip to LEDGER before the next one's offset can safely advance on that partition) — acceptable, since correctness under crash/redelivery is the property being proven, not raw throughput.

## Decision

**Chosen option: Option B.** At-least-once broker delivery, keyed for per-account ordering, plus an idempotent downstream API turns into effectively-once posting — the same pattern (broker retries + idempotent receiver) used by real production event pipelines, made concrete and testable here instead of asserted.

## Consequences

**Positive:**
- The correctness claim ("no event is lost, none is double-posted") reduces to two already-independently-testable facts: Kafka/Redpanda's documented at-least-once behavior, and LEDGER's already-tested idempotency guarantee — WIRE's own tests (ADR-004) only need to prove the wiring between them is correct, not reprove either guarantee from scratch
- Per-account ordering is enforced structurally (partition key), so it can't be silently broken by an unrelated future code change the way an application-level sequencing scheme could be

**Negative / accepted tradeoffs:**
- Throughput on a single hot account is bounded by LEDGER's response latency, not by the broker — accepted, since unbounded per-account throughput isn't the claim being made (LEDGER's own `SELECT ... FOR UPDATE` locking already serializes writes to one account regardless of how fast WIRE could produce them)
- If `eventId` generation upstream is ever non-unique (a producer bug), that failure mode is invisible to WIRE — it would just look like a normal idempotent-retry to LEDGER. Tracked as an accepted trust boundary in `docs/RISKS.md`

**Risks:**
- Manual offset commit is easy to get subtly wrong (committing too early, or on the wrong condition) — this is exactly what ADR-004's tests need to catch, not just describe

## Notes

`eventId` is the field name on WIRE's event schema; it is passed to LEDGER verbatim as `idempotencyKey`. Keeping the name distinct in WIRE's own schema (rather than calling it `idempotencyKey` end-to-end) is deliberate — it's an identity WIRE receives from upstream, not a value WIRE invents, and the rename at the LEDGER-call boundary makes that provenance visible in the code.
