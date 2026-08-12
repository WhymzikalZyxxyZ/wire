# WIRE

An event-ingestion pipeline — Java, Spring Kafka, Redpanda (Kafka-protocol) — that gets real-time transaction events into [LEDGER](https://github.com/WhymzikalZyxxyZ/ledger) reliably, in order, and idempotently.

LEDGER proves synchronous correctness: once a transaction request has arrived, it's posted atomically, idempotently, and without lost updates under concurrency. WIRE proves the other half of the same resume line — *"designed high-throughput backend services to ingest, process, and store real-time cash flow and transaction data with strict reliability and latency requirements"* — the part about getting events from an upstream source into that system reliably in the first place, via a real event-streaming broker rather than a single synchronous call per event.

> ⚠️ **This is a portfolio demonstration, not audited financial infrastructure.** It must never be pointed at a real payment processor or represented as production-ready without a real security review and correctness audit. See [`docs/RISKS.md`](docs/RISKS.md).

## Status

This repository currently contains:
- A minimal, buildable Spring Boot skeleton (`src/main/java/`) with no consumer, producer, or broker wiring yet
- Full design documentation (this README, four ADRs, an architecture overview, a risk register)

Not yet built: the actual Kafka consumer/producer, the LEDGER-calling logic, and the correctness tests that would prove the claims below.

## Why these choices — and what each one is proving

| Decision | Choice | What it's proving |
|---|---|---|
| Broker | Redpanda (Kafka-wire-protocol-compatible) | Real partition/consumer-group/offset semantics — the actual mechanics of high-throughput event streaming, not a simplified queue ([ADR-001](docs/adr/001-stack-and-broker.md)) |
| Delivery model | At-least-once broker delivery + LEDGER's existing idempotency guarantee | Effectively-once posting, built on a guarantee that's already independently proven, not a second idempotency system invented from scratch ([ADR-002](docs/adr/002-delivery-and-consistency.md)) |
| Ordering | Partition key = accountId | Per-account ordering as a structural property of the partitioning scheme, not application-level bookkeeping that a future change could quietly break ([ADR-002](docs/adr/002-delivery-and-consistency.md)) |
| Failure handling | Bounded retry + backoff for transient failures, immediate dead-letter routing for terminal ones | "Strict reliability" under real failure conditions — self-healing retries, and no single poison message can block an entire partition ([ADR-003](docs/adr/003-failure-handling.md)) |
| Correctness verification | Integration tests against a real Redpanda broker (Testcontainers), with LEDGER's API contract stubbed at the boundary | The difference between *designed* correct and *proven* correct — not yet closed, tracked explicitly ([ADR-004](docs/adr/004-correctness-verification.md)) |

## Architecture

See [`docs/architecture/overview.md`](docs/architecture/overview.md) for the topic layout, event schema, and consumer flow end to end.

## Risks & known gaps

See [`docs/RISKS.md`](docs/RISKS.md) — read before treating any correctness claim here as more than design intent.

## Building

```
git clone https://github.com/WhymzikalZyxxyZ/wire.git
cd wire
mvn compile
```

Requires JDK 21 and Maven. No broker connection is configured yet, so `mvn spring-boot:run` will not yet start a working application — that arrives with the first consumer/producer implementation.

## License

MIT — see [LICENSE](LICENSE).
