# WIRE — Risk & Gap Register

Living document. Update as decisions are made or new risks surface.

## This is a portfolio demonstration, not audited financial infrastructure — CRITICAL

Same posture as LEDGER's risk register, and for the same reason: everything here exists to demonstrate engineering competence, not to move real money or ingest real financial events.

**Mitigation stance:** the README must state this plainly. **This system must never be pointed at a real payment processor or represented as production-ready without a real security review and correctness audit beyond this project's own test suite.** Same non-negotiable posture as LEDGER.

## Coupling to LEDGER's API contract is untracked — MEDIUM (accepted gap)

WIRE's consumer calls LEDGER's `POST /transactions` and depends on its exact request/response shape and status-code semantics (see LEDGER's `TransactionController`). Nothing enforces that WIRE's stub in its own tests (ADR-004) stays in sync with LEDGER's real contract if LEDGER changes.

**Mitigation stance:** accepted for a two-repository personal project of this scale. A real system would use a shared schema/contract-testing tool (e.g., a shared OpenAPI spec with contract verification in both repos' CI) — noted here as a real hardening step, not built, because it would meaningfully expand scope for a demonstration project.

## No real upstream event source — LOW (explicitly out of scope, not hidden)

The producer side of this system is a synthetic event simulator, not an integration with an actual bank, card network, or payment processor. "Real-time cash flow ingestion" is demonstrated structurally (the pipeline, the delivery guarantees, the failure handling), not against a genuine external data source.

**Mitigation stance:** accepted scope boundary, named explicitly in the README so it reads as a deliberate choice.

## Redpanda vs. Kafka fidelity — LOW (documented divergence)

Per [ADR-001](adr/001-stack-and-broker.md), Redpanda is Kafka-wire-protocol-compatible but not identical to Kafka in every operational respect (tooling, some broker-internal behaviors).

**Mitigation stance:** accepted — the properties under test (partitioning, consumer groups, offsets) are protocol-level and unaffected. A future pass could re-verify against real Kafka in CI if that operational fidelity ever became load-bearing.

## Delivery/ordering/failure-handling correctness is designed but not yet proven — HIGH (tracked explicitly, not hidden)

ADR-002 and ADR-003 make specific claims (per-account ordering, no double-posting on redelivery, poison messages don't block a partition, transient failures self-heal). ADR-004 commits to proving these with real tests against a real broker.

**Mitigation stance:** **as of this documentation phase, those tests don't exist yet, because no consumer/producer code exists yet either.** Until they're written and passing, treat the claims in ADR-002/003 as design intent, not verified fact — same discipline LEDGER applied at the same stage, and the same reason LEDGER's ADR-004 turned out to matter: the first real implementation pass caught a bug no design document would have.

## DLQ reprocessing is undesigned — MEDIUM (open item)

Per `docs/architecture/overview.md`, events land on `wire.transactions.dlq` on terminal failure, but there's no designed mechanism yet for inspecting, correcting, and replaying them back into the pipeline.

**Mitigation stance:** gap, not yet resolved. Not blocking for the documentation phase, but a real ingestion system needs an answer here before this claims to model production operations.
