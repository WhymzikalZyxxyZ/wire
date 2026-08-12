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

## Failure-handling correctness is now proven by tests for the scenarios that matter most — RESOLVED for retry/DLQ (was HIGH)

ADR-003's retry/DLQ claims (transient failures self-heal, terminal failures route to the DLQ without retry, exhausted retries dead-letter cleanly) are proven by `TransactionEventFlowIntegrationTest` against a real Redpanda broker with LEDGER's HTTP boundary stubbed.

**Mitigation stance:** resolved for those four scenarios. See the next two entries for what's explicitly **not** independently re-verified by this suite.

## Topic partitioning and consumer concurrency were unconfigured — RESOLVED (was undocumented)

A code-level survey found that neither `wire.transactions.raw`'s partition count nor the consumer's listener concurrency were ever explicitly set. Left to defaults, the broker would create the topic with (typically) a single partition, and `ConcurrentKafkaListenerContainerFactory` defaults to `concurrency=1` — meaning ADR-002's accountId partition key would have had nothing to partition across, silently defeating the entire ordering scheme in both production config and tests, without anything in the code or docs saying so.

**Mitigation stance:** resolved. `KafkaConfig` now declares `NewTopic` beans (`wire.topics.raw-partitions`, default 3) provisioned automatically via Spring's `KafkaAdmin`, and `kafkaListenerContainerFactory` sets `concurrency` (`wire.consumer.concurrency`, default 3, matched to the partition count) so multiple consumer threads genuinely process different partitions concurrently rather than one thread serializing everything.

## True multi-partition ordering is not independently tested — MEDIUM (accepted, narrowed scope)

ADR-002 claims per-partition-key ordering. Now that the topology genuinely has multiple partitions and multiple consumer threads (see above), WIRE's test suite still does not run a load that would provoke and assert on relative event ordering across concurrent partitions — that would need meaningfully more test infrastructure than this pass built (see ADR-004's Decision). What WIRE relies on instead is Kafka/Redpanda's own documented per-partition ordering guarantee, applied structurally via the now-real partitioning scheme.

**Mitigation stance:** accepted. The partitioning *scheme* is now genuinely load-bearing (not inert, as it was before the fix above); the *guarantee it depends on* is still Kafka's own, not re-derived here. A future pass adding a genuine ordering test (multiple producers, one partition, asserted processing order) would close this gap outright.

## Crash-then-redelivery is not independently simulated — MEDIUM (accepted, narrowed scope)

ADR-002 claims a consumer crash before offset commit is safe (the broker redelivers, and LEDGER's idempotency key makes the repeat a no-op). WIRE's test suite doesn't kill a JVM mid-processing to force this — a single-process JUnit suite can't do that cleanly. What's actually tested is the piece that matters: LEDGER's own `TransactionServiceIntegrationTest.resubmittingTheSameIdempotencyKeyReturnsTheOriginalResultWithoutDoublePosting` already proves a repeated call with the same idempotency key is safe. WIRE's claim rests on that, not a re-simulation of the crash itself.

**Mitigation stance:** accepted — this is the intended design (ADR-002 explicitly builds on LEDGER's already-proven guarantee rather than reproving it). A meaningfully stronger test here would need multi-process or container-restart-based test infrastructure, which is a real but disproportionate investment for what it would add.

## Retries block the consumer thread during backoff — LOW (accepted implementation tradeoff)

Per ADR-003's Notes, retries are a blocking loop (`Thread.sleep` between attempts) inside the message handler, not Spring Kafka's non-blocking retry-topic feature. During a backoff window, that consumer thread isn't servicing other partitions it may also own.

**Mitigation stance:** accepted — chosen deliberately for simplicity and testability over the more idiomatic-but-harder-to-verify-blind `@RetryableTopic` approach. Named as a real upgrade path, not a hidden limitation, if consumer throughput under partial outages ever became the actual bottleneck.

## LedgerClient had no request timeout — RESOLVED (was undocumented)

A code-level survey found `LedgerClient`'s `RestClient` had no connect or read timeout configured. Without one, a LEDGER instance that hangs (accepts the connection but never responds) would never throw `ResourceAccessException` — the consumer thread would block indefinitely instead of ever reaching the transient-failure retry path ADR-003 describes.

**Mitigation stance:** resolved. `LedgerClient` now builds its `RestClient` with an explicit `SimpleClientHttpRequestFactory` (`wire.ledger.connect-timeout-ms` / `read-timeout-ms`, defaulting to 5s/10s) — a timeout now reliably surfaces as `ResourceAccessException`, which was already mapped to the transient/retry path.

## No escalation if the DLQ publish itself fails — RESOLVED, with an accepted tradeoff (was undocumented)

A code-level survey found that `DeadLetterPublisher.publish()` threw an uncaught exception straight out of the listener's message handler, before `acknowledgment.acknowledge()` ever ran. If the DLQ broker path itself were unreachable (not just LEDGER), the event would redeliver forever and block that partition indefinitely — the exact head-of-line-blocking failure mode ADR-003 exists to prevent, reintroduced one layer deeper than it was designed for.

**Mitigation stance:** resolved, with a real, named tradeoff. `TransactionEventListener` now catches this specific failure (`DeadLetterPublishException`), logs it at ERROR with a "manual recovery required" marker, and still acknowledges the offset — trading guaranteed DLQ durability for avoiding an infinite redelivery lock-up in this rare double-failure case (LEDGER *and* the DLQ path both unreachable at once). The failure is still recorded, just in application logs rather than the durable DLQ topic. A real production system would want a true last-resort sink (local disk, a paging alert) here — that remains a real gap, not solved by this fix, just no longer silent.

## No REST-layer error handling for a producer publish failure — RESOLVED (was undocumented)

A further code survey found `EventController`'s `POST /events` had no handler for a Kafka publish failure (the broker unreachable, or an unacknowledged send) — it fell through to Spring Boot's default, unstructured error response instead of this API's own `ErrorResponse` shape.

**Mitigation stance:** resolved. `EventProducer.publish()` now throws a dedicated `EventPublishException`, mapped by `ApiExceptionHandler` to `503 Service Unavailable` (not `500`) — the request itself was valid; the broker WIRE depends on wasn't reachable, a meaningfully different and retryable condition for the caller. A catch-all `Exception` handler was also added so any other unexpected failure still returns the consistent error shape rather than leaking implementation details.

## POST /events was unauthenticated — RESOLVED (was HIGH, found by a security audit)

A dedicated security-auditor pass found `POST /events` had no authentication, authorization, or rate limiting at all — no Spring Security dependency existed anywhere in the project. Anyone with network access could submit arbitrary fabricated `WireTransactionEvent` payloads, which flow through Kafka and get forwarded to LEDGER's `POST /transactions` verbatim, with a caller-chosen `eventId` used as LEDGER's idempotency key. This was the single most concrete, immediately-exploitable finding in the audit.

**Mitigation stance:** resolved with a lightweight fix proportionate to the gap — a `ProducerApiKeyFilter` checking a shared-secret `X-API-Key` header (`wire.producer.api-key`, no default, fails fast) via constant-time comparison, scoped to `/events`. Full Spring Security wasn't pulled in for a single header check — this is a service-to-service ingestion point, not a public-facing UI, and a shared secret is the right amount of mechanism for that. A production deployment with multiple legitimate producers would want per-caller API keys or mTLS instead of one shared secret; noted as a real next step, not solved by this fix.

## Unbounded event payloads — RESOLVED (was MEDIUM, found by a security audit)

The same audit found `WireTransactionEvent.entries` had no upper bound (mirrors the identical finding already fixed in LEDGER), `eventId`/`description` had no length bound, and `WireEntry.amount`/`currency` had no precision/format bounds — matching gaps to LEDGER's own, since WIRE's event shape mirrors LEDGER's request shape closely.

**Mitigation stance:** resolved. Added `@Size(max = 100)` to `entries`, `@Size(max = 255)` to `eventId`, `@Size(max = 2000)` to `description`, `@Digits(integer = 15, fraction = 4)` to `amount`, and `@Pattern(regexp = "^[A-Z]{3}$")` to `currency` — bounds chosen to match LEDGER's exactly, so a request that would be rejected at LEDGER's boundary is now rejected at WIRE's boundary first.

## `eventId` had no charset restriction — log-forging vector — RESOLVED (was LOW, found by a security audit)

`eventId` is caller-controlled and gets interpolated into SLF4J log lines in `TransactionEventListener` with no sanitization. A caller could embed CRLF/control characters to forge fake log lines.

**Mitigation stance:** resolved. `eventId` is now restricted to `^[A-Za-z0-9_:.-]+$` — control characters (and anything else that isn't a normal identifier character) are rejected at the validation boundary, before the value is ever logged.

## `JsonDeserializer.trustedPackages("*")` — RESOLVED (was LOW, found by a security audit)

Both Kafka consumer factories (`KafkaConfig` for `WireTransactionEvent`, and the test-only `DlqTestConfig` for `DeadLetterEnvelope`) trusted every package on the classpath for deserialization. The audit's own verdict: this was low-risk *today* specifically because the producer disables type-info headers (`setAddTypeInfo(false)`), so the deserializer always falls back to its fixed target class regardless of the trust list — exploiting the wildcard would require an attacker who can already write raw bytes with a forged `__TypeId__` header directly onto the topic, a materially higher bar than the REST API.

**Mitigation stance:** resolved anyway, since narrowing costs nothing. Both deserializers now declare only the specific packages they actually deserialize (`xyz.zyxwonderland.wire.event`, and `xyz.zyxwonderland.wire.dlq`/`xyz.zyxwonderland.wire.event` for the DLQ envelope) — removes the risk outright rather than relying on the type-info-header assumption holding forever.

## DLQ reprocessing is undesigned — MEDIUM (open item)

Per `docs/architecture/overview.md`, events land on `wire.transactions.dlq` on terminal failure, but there's no designed mechanism yet for inspecting, correcting, and replaying them back into the pipeline.

**Mitigation stance:** gap, not yet resolved. A real ingestion system needs an answer here before this claims to model production operations.
