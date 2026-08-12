# ADR-003: Failure Handling

**Date:** 2026-08-12
**Status:** Accepted
**Deciders:** Zyxxyz

---

## Context

Two structurally different kinds of failure can happen while processing an event, and treating them the same is a design mistake: a transient failure (LEDGER momentarily unreachable, a timeout, a 503) means "try this exact event again later, it will probably work" — but a poison message (malformed JSON, a reference to an `accountId` that doesn't exist, a validation failure LEDGER itself rejects with a 4xx) means "this event will never succeed no matter how many times it's retried," and retrying it forever would stall every event behind it on the same partition.

## Decision Drivers

- Retrying a transient failure should not require operator intervention
- A poison message must not be able to block an entire partition indefinitely (head-of-line blocking is a real production incident, not a theoretical concern)
- Failure handling needs to be visible — a message that silently vanishes into a retry loop or a dead letter topic with no observability defeats the purpose of proving "reliability"

## Options Considered

### Option A — Retry forever, in place, on the same partition
On any failure, retry the same event indefinitely before advancing the offset.

**Pros:** Simple; never loses or skips an event.
**Cons:** A single poison message (malformed data, a permanently-invalid account reference) blocks every event behind it on that partition forever — the exact head-of-line-blocking failure mode real systems are designed specifically to avoid.

### Option B — Bounded retry with backoff for transient failures, dead-letter topic for terminal failures
Classify each failure at the point LEDGER's response comes back. Transient failures (network error, timeout, 5xx) retry with exponential backoff up to a bounded attempt count. Terminal failures (validation errors, 4xx responses, malformed events that fail schema validation before ever reaching LEDGER) are published to a `wire.transactions.dlq` topic immediately — no retry — along with the failure reason, and the offset advances so the partition keeps moving.

**Pros:** Transient failures self-heal without operator involvement; a poison message is quarantined in one step instead of blocking the pipeline; the DLQ becomes a queryable record of exactly what failed and why, which is itself part of "reliability" — failures need to be visible and inspectable, not just survived.
**Cons:** Requires a real failure taxonomy (deciding which HTTP statuses and exceptions are transient vs. terminal) rather than one uniform retry policy — more design surface, but that surface is exactly the thing worth demonstrating.

## Decision

**Chosen option: Option B.** Bounded retry with backoff for transient failures; immediate dead-letter routing for terminal failures, with the offset always advancing so no single bad event can stall a partition.

Explicitly out of scope: a transactional/exactly-once Kafka producer chain (Kafka's own transactional API, used when a service both consumes and produces within one atomic unit). WIRE doesn't need it — the correctness guarantee here comes from the idempotent LEDGER boundary (ADR-002), not from broker-level transactionality, and adding it would prove a different, unrelated capability rather than strengthen this one.

## Consequences

**Positive:**
- No single malformed or permanently-invalid event can halt processing for every other event behind it on the same partition
- The DLQ is a first-class, inspectable record of what failed and why — closer to how a real operations team would actually need to triage ingestion failures
- Transient-failure retries need no manual intervention, which is the actual "reliability" claim this project targets

**Negative / accepted tradeoffs:**
- The transient-vs-terminal classification is a judgment call baked into code (e.g., "is a connection timeout retryable but a 400 not") — a real system might need this tuned per failure type over time; accepted as a reasonable default, not treated as a permanently-solved taxonomy
- Events on the DLQ require a separate reprocessing story (manually replaying a fixed-and-corrected event) that isn't built in this pass — tracked in `docs/RISKS.md`

**Risks:**
- If retry backoff is misconfigured (too aggressive), it could itself create load-related transient failures against LEDGER — worth exercising under WIRE's own test suite (ADR-004), not just trusting the backoff math on paper

## Notes

The specific retry-count and backoff parameters are an implementation detail settled when the consumer is actually built, not fixed here — this ADR commits to the retry/DLQ *shape*, not exact numbers.
