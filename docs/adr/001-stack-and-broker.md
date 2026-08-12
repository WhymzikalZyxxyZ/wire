# ADR-001: Stack & Broker Choice

**Date:** 2026-08-12
**Status:** Accepted
**Deciders:** Zyxxyz

---

## Context

WIRE exists to prove the half of the "ingest, process, and store real-time cash flow and transaction data with strict reliability and latency requirements" resume line that [LEDGER](https://github.com/WhymzikalZyxxyZ/ledger) doesn't cover: LEDGER proves synchronous, request/response correctness (idempotency, no lost updates) once a transaction has arrived. WIRE proves the other half — getting transaction events from an upstream source into LEDGER reliably, in order, and at throughput, via an event-streaming broker rather than a direct synchronous call per event.

## Decision Drivers

- The language/runtime should let WIRE call LEDGER's existing REST API directly and share its domain vocabulary (accounts, entries, idempotency keys) without translation-layer friction
- The broker needs real partition/consumer-group/offset semantics — the properties actually being proven (ordering, replay, at-least-once delivery) don't exist in a simple point-to-point queue
- Whatever broker is chosen has to be verifiable the same way LEDGER's concurrency claims were: real integration tests against a real broker in CI, not mocks — see [ADR-004](004-correctness-verification.md). That means CI startup time and Testcontainers support are real decision inputs, not afterthoughts

## Options Considered

### Option A — Java + Spring Boot, calling LEDGER's REST API directly
Same stack as LEDGER. Spring Kafka's client works unmodified against any Kafka-wire-protocol broker (Kafka itself, Redpanda, etc.), so this decision is independent of the broker choice below.

**Pros:** Zero translation friction with LEDGER's DTOs and domain concepts; realistic pairing (a real ingestion service calling a real ledger service usually shares a platform, or at least the same HTTP client conventions); no new build tooling to learn.
**Cons:** Doesn't add a new language to the portfolio — but the language portfolio (nine chess engines) already proves polyglot range, so that's not a gap this project needs to fill.

### Option B — Go, matching `editor-service`
A second Go service, reusing that stack's proven "high-throughput" framing.

**Pros:** Go's goroutine model is a natural fit for a concurrent consumer; reuses infrastructure knowledge from `editor-service`.
**Cons:** Would require reimplementing LEDGER's request/response DTOs in a second language for no functional benefit — the point of WIRE is proving event-ingestion design, not proving Go competence a second time.

## Decision

**Chosen option: Option A — Java + Spring Boot.** Broker: **Redpanda**, a Kafka-wire-protocol-compatible broker, over Apache Kafka itself or a simple queue (AWS SQS/LocalStack).

Redpanda speaks the exact same client protocol Kafka does — the same partition/consumer-group/offset/replay semantics this project exists to demonstrate — so nothing about the architecture, the client code, or the correctness claims changes versus running real Kafka. What changes is operational weight: Redpanda ships as a single binary with no ZooKeeper/KRaft cluster to stand up, and has first-class Testcontainers support, which matters directly after [LEDGER's ADR-004](https://github.com/WhymzikalZyxxyZ/ledger/blob/main/docs/adr/004-correctness-verification.md) already had to fix a real CI-lifecycle bug with a heavier piece of test infrastructure. A simple queue (SQS/LocalStack) was ruled out entirely: SQS has no partition-ordering or consumer-group-rebalancing model, so it can't demonstrate the specific properties (per-key ordering, replay, coordinated multi-consumer scaling) that are the actual point of this project.

## Consequences

**Positive:**
- WIRE's Kafka-protocol consumer code, tests, and operational lessons transfer directly to a real Kafka deployment — this is not a toy substitute, it's the same protocol running on lighter infrastructure
- Shares idempotency-key and DTO vocabulary with LEDGER's `SubmitTransactionRequest`/`EntryRequest`, so the two repos read as one coherent system rather than two unrelated demos
- Fast, self-contained CI: a single Redpanda container starts in seconds, no external broker cluster or account required

**Negative / accepted tradeoffs:**
- Redpanda is not byte-for-byte identical to Kafka in every operational edge case (e.g., some Kafka-specific admin tooling, exact broker-side tiered-storage behavior) — accepted, since the properties under test here are protocol-level (partitioning, offsets, consumer groups), not broker-internals-level
- A reviewer skimming quickly may read "Redpanda" as less recognizable than "Kafka" — mitigated by naming Kafka-protocol-compatibility explicitly in the README, since that's the actual claim being made

**Risks:**
- None specific to this decision; tracked generally in `docs/RISKS.md`

## Notes

See [ADR-002](002-delivery-and-consistency.md) for how delivery guarantees are actually achieved on top of this broker choice, and [ADR-004](004-correctness-verification.md) for how that gets proven rather than asserted.
