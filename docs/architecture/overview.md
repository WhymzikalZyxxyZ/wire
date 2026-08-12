# WIRE — Architecture Overview

See `docs/adr/` for the reasoning behind each decision referenced here.

## Topics

```
wire.transactions.raw
  Producer-facing topic. One event per attempted transaction.
  Partition key: accountId  (ADR-002 — guarantees per-account ordering)

wire.transactions.dlq
  Terminal-failure sink. Malformed events and events LEDGER rejects
  outright (validation failures) land here instead of blocking
  wire.transactions.raw's partitions. (ADR-003)
```

## Event schema (`wire.transactions.raw`)

```json
{
  "eventId": "a stable, producer-assigned unique id — becomes LEDGER's idempotencyKey verbatim",
  "accountId": "uuid — also the partition key",
  "amount": "signed decimal string — positive = debit, negative = credit, matching LEDGER's convention",
  "currency": "ISO 4217, e.g. USD",
  "description": "free text",
  "occurredAt": "ISO-8601 timestamp — when the event happened upstream, not when WIRE saw it"
}
```

A real transaction is two or more of these events sharing a correlating field (e.g. a `transferId`) that nets to zero — WIRE's consumer groups related events into one `SubmitTransactionRequest` before calling LEDGER, since LEDGER's API takes a whole balanced transaction per call, not one entry at a time. (The exact correlation mechanism is an implementation-phase decision; this document commits to the two-sided nature of the data, not the grouping algorithm yet.)

## Consumer flow

1. Poll `wire.transactions.raw`. Events for a given `accountId` always arrive on the same partition, in production order (ADR-002).
2. Validate the event against the schema above. Malformed → publish to `wire.transactions.dlq` with a failure reason, advance the offset, continue. (ADR-003)
3. Group correlated events into a `SubmitTransactionRequest`, with `eventId` (or the group's correlating id) mapped to `idempotencyKey`.
4. `POST` to LEDGER's `/transactions`.
   - **2xx (created or already-existed):** success. Advance the offset.
   - **4xx (validation failure, e.g. unbalanced entries or unknown account):** terminal. Publish to `wire.transactions.dlq` with LEDGER's error response attached. Advance the offset. (ADR-003)
   - **5xx / network error / timeout:** transient. Retry with exponential backoff, up to a bounded attempt count. Only advance the offset after a terminal outcome (success or DLQ). (ADR-002, ADR-003)
5. If the consumer crashes at any point before step 4 reaches a terminal outcome and its offset commit, the broker redelivers the event on restart. Because `eventId` → `idempotencyKey` is stable, the redelivered call to LEDGER either creates the transaction (if it never actually completed) or gets LEDGER's existing-result response (if it did) — never a duplicate posting. (ADR-002)

## What's built

Nothing yet beyond a buildable skeleton — see `docs/RISKS.md`. This document describes the target design the ADRs commit to.

## Explicitly not built

- The event-correlation/grouping algorithm that turns N raw events into one balanced `SubmitTransactionRequest` (noted above as an implementation-phase decision)
- A real upstream event source — the producer side is a synthetic simulator for demonstration, not an integration with an actual payment processor (see `docs/RISKS.md`)
- DLQ reprocessing/replay tooling
- Consumer lag / DLQ depth metrics and alerting
