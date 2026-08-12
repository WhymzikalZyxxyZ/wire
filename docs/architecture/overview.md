# WIRE — Architecture Overview

See `docs/adr/` for the reasoning behind each decision referenced here.

## Topics

```
wire.transactions.raw
  Producer-facing topic. One message = one already-balanced transaction
  (its full list of entries), not one entry per message — see "Event
  schema" below for why. Partition key: the transaction's first entry's
  accountId (ADR-002).

wire.transactions.dlq
  Terminal-failure sink. Events LEDGER rejects outright (validation
  failures) and events that exhaust their transient-failure retries land
  here instead of blocking wire.transactions.raw's partitions. (ADR-003)
```

## Event schema (`wire.transactions.raw`)

```json
{
  "eventId": "a stable, producer-assigned unique id — becomes LEDGER's idempotencyKey verbatim",
  "description": "free text",
  "entries": [
    { "accountId": "uuid", "amount": "signed decimal, positive = debit, negative = credit", "currency": "ISO 4217, e.g. USD" }
  ],
  "occurredAt": "ISO-8601 timestamp — when the event happened upstream, not when WIRE saw it"
}
```

**Resolved design decision** (this was originally left open as "an implementation-phase decision"): one Kafka message carries one whole balanced transaction — its full `entries` list — rather than one message per entry with a separate correlation step to reassemble them. This maps almost 1:1 onto LEDGER's own `SubmitTransactionRequest` and avoids needing a stateful correlator (buffering partial transactions by some `transferId` until a complete, balanced set has arrived, with its own timeout/incomplete-transfer failure mode). The tradeoff, named honestly rather than hidden: this assumes upstream sources emit whole transactions, not individual legs recorded by separate systems that would need reassembling — a real integration with a genuinely fragmented upstream source might need the correlator this design avoided. See `docs/RISKS.md`.

**Partition key.** Every entry within one event is guaranteed co-located, since they arrive in a single message. But two different transactions that both touch the same account only land on the same partition if they happen to pick the same entry as "first" — this scheme does not give an account true global ordering across every transaction it's ever part of, only within transactions that share a partition key. Named as an accepted limitation, not a broken promise: see `docs/RISKS.md` and [ADR-002](../adr/002-delivery-and-consistency.md)'s Notes.

## Consumer flow

1. Poll `wire.transactions.raw`. Manual offset commit only — never on a timer (`ENABLE_AUTO_COMMIT_CONFIG=false`).
2. `POST` to LEDGER's `/transactions`, with `eventId` sent as `idempotencyKey`.
   - **2xx:** success (created or already-existed, from a prior redelivered attempt). Acknowledge the offset.
   - **4xx (validation failure — e.g. unbalanced entries or unknown account):** terminal. Publish to `wire.transactions.dlq` with LEDGER's error response attached, then acknowledge the offset. No retry — a 4xx will fail identically every time. (ADR-003)
   - **5xx / network error / timeout:** transient. Retry with exponential backoff (default: 3 retries after the initial attempt, doubling from a 200ms base — 4 total calls to LEDGER before giving up). If every retry is exhausted, publish to `wire.transactions.dlq` and acknowledge the offset. (ADR-003)
3. The DLQ publish itself is synchronous and durably confirmed (waits on the broker's send acknowledgment) before the original offset is acknowledged — a crash between "DLQ send() called" and actual delivery must not silently commit an offset for a failure that was never actually recorded.
4. If the consumer crashes at any point before a terminal outcome is reached and acknowledged, the broker redelivers the event on restart. Because `eventId` → `idempotencyKey` is stable, the redelivered call to LEDGER either creates the transaction (if it never actually completed) or gets LEDGER's existing-result response (if it did) — never a duplicate posting. (ADR-002)

**Implementation choice:** retries block the consumer thread for the duration of the backoff (a small in-process loop with `Thread.sleep`), rather than using Spring Kafka's non-blocking retry-topic feature (`@RetryableTopic`, which republishes failed messages to separate retry topics instead of blocking the original partition). The simpler approach was chosen to keep the retry/DLQ behavior fully within application code — directly testable and easy to reason about without depending on Spring Kafka's topic-auto-creation machinery — at the cost of that consumer thread's other partitions (if concurrency > 1) not being serviced during a backoff window. Named as a real, accepted tradeoff in `docs/RISKS.md`, not a silent simplification.

## Producer flow

`POST /events` on WIRE itself accepts an event matching the schema above, defaults `occurredAt` to now if omitted, and publishes it to `wire.transactions.raw` — waiting for the broker's send acknowledgment before returning, so a caller that gets a response knows the event is durably queued. This stands in for a real upstream integration (a Kafka Connect source connector, or an SDK call from an actual payment system) — see `docs/RISKS.md`'s "no real upstream event source" entry.

## What's built

The full flow above: `EventProducer`/`EventController` (producer), `TransactionEventListener`/`LedgerClient`/`DeadLetterPublisher`/`RetryPolicy` (consumer), and `KafkaConfig` wiring both to a Kafka-protocol broker via explicit `ProducerFactory`/`ConsumerFactory` beans. `TransactionEventFlowIntegrationTest` proves the retry/DLQ contract described above against a real Redpanda broker (Testcontainers) with LEDGER's HTTP boundary stubbed (WireMock) — see [ADR-004](../adr/004-correctness-verification.md) for exactly what is and isn't covered by that suite.

## Explicitly not built

- A stateful correlator for reassembling transactions from separately-produced per-leg events (see "Resolved design decision" above — deliberately avoided, not deferred)
- A real upstream event source — the producer side is a synthetic REST-triggered simulator, not an integration with an actual payment processor
- DLQ reprocessing/replay tooling
- Consumer lag / DLQ depth metrics and alerting
- A shared, verified contract between WIRE's `LedgerTransactionRequest`/`LedgerTransactionResponse` DTOs and LEDGER's real API (currently kept in sync by hand — see `docs/RISKS.md`)
