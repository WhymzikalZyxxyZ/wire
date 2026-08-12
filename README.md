# WIRE

An event-ingestion pipeline — Java, Spring Kafka, Redpanda (Kafka-protocol) — that gets real-time transaction events into [LEDGER](https://github.com/WhymzikalZyxxyZ/ledger) reliably, in order, and idempotently.

LEDGER proves synchronous correctness: once a transaction request has arrived, it's posted atomically, idempotently, and without lost updates under concurrency. WIRE proves the other half of the same resume line — *"designed high-throughput backend services to ingest, process, and store real-time cash flow and transaction data with strict reliability and latency requirements"* — the part about getting events from an upstream source into that system reliably in the first place, via a real event-streaming broker rather than a single synchronous call per event.

> ⚠️ **This is a portfolio demonstration, not audited financial infrastructure.** It must never be pointed at a real payment processor or represented as production-ready without a real security review and correctness audit. See [`docs/RISKS.md`](docs/RISKS.md).

## Status

This repository currently contains:
- Full design documentation (this README, four ADRs, an architecture overview, a risk register)
- A working producer (`POST /events`) and consumer (`TransactionEventListener`) implementing the flow in [`docs/architecture/overview.md`](docs/architecture/overview.md), including bounded retry with backoff and dead-letter routing
- Integration tests (`TransactionEventFlowIntegrationTest`) proving the retry/DLQ contract against a real Redpanda broker (Testcontainers) with LEDGER's HTTP boundary stubbed — see [ADR-004](docs/adr/004-correctness-verification.md) for exactly what is and isn't covered

Not yet built: a real upstream event source (the producer is a synthetic REST-triggered simulator), DLQ reprocessing/replay tooling, and deployment to Redpanda Cloud/Fly.io.

## Why these choices — and what each one is proving

| Decision | Choice | What it's proving |
|---|---|---|
| Broker | Redpanda (Kafka-wire-protocol-compatible) | Real partition/consumer-group/offset semantics — the actual mechanics of high-throughput event streaming, not a simplified queue ([ADR-001](docs/adr/001-stack-and-broker.md)) |
| Delivery model | At-least-once broker delivery + LEDGER's existing idempotency guarantee | Effectively-once posting, built on a guarantee that's already independently proven, not a second idempotency system invented from scratch ([ADR-002](docs/adr/002-delivery-and-consistency.md)) |
| Event shape | One message = one whole balanced transaction | Avoids a stateful correlator to reassemble per-leg events, at a named tradeoff — see the architecture doc's "Resolved design decision" ([ADR-002](docs/adr/002-delivery-and-consistency.md)) |
| Failure handling | Bounded retry + backoff for transient failures, immediate dead-letter routing for terminal ones | "Strict reliability" under real failure conditions — self-healing retries, and no single poison message can block an entire partition. Proven by `TransactionEventFlowIntegrationTest`, not just designed ([ADR-003](docs/adr/003-failure-handling.md)) |
| Correctness verification | Integration tests against a real Redpanda broker (Testcontainers), with LEDGER's API contract stubbed at the boundary | The difference between *designed* correct and *proven* correct — closed for the retry/DLQ contract; ordering and crash-redelivery remain inherited guarantees, named explicitly rather than re-claimed ([ADR-004](docs/adr/004-correctness-verification.md)) |

## Architecture

See [`docs/architecture/overview.md`](docs/architecture/overview.md) for the topic layout, event schema, and consumer flow end to end.

## Risks & known gaps

See [`docs/RISKS.md`](docs/RISKS.md) — read before treating any correctness claim here as more than what's actually been tested.

## Building

```
git clone https://github.com/WhymzikalZyxxyZ/wire.git
cd wire
mvn compile
```

Requires JDK 21 and Maven. Running the app (`mvn spring-boot:run`) requires `KAFKA_BOOTSTRAP_SERVERS`, `LEDGER_BASE_URL`, and `WIRE_PRODUCER_API_KEY` set — no defaults are provided, by design (see [`application.yml`](src/main/resources/application.yml)). `WIRE_PRODUCER_API_KEY` is the shared secret `POST /events` requires in an `X-API-Key` header (see [`docs/RISKS.md`](docs/RISKS.md)). Running the test suite (`mvn test`) requires Docker, for Testcontainers.

## License

MIT — see [LICENSE](LICENSE).
