package xyz.zyxwonderland.wire.consumer;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import xyz.zyxwonderland.wire.AbstractIntegrationTest;
import xyz.zyxwonderland.wire.DlqCapture;
import xyz.zyxwonderland.wire.dlq.DeadLetterEnvelope;
import xyz.zyxwonderland.wire.event.WireEntry;
import xyz.zyxwonderland.wire.event.WireTransactionEvent;
import xyz.zyxwonderland.wire.producer.EventProducer;

/**
 * Proves ADR-003's claims end to end: a successful event never touches
 * the DLQ, a validation rejection routes there immediately with no
 * retry, a transient failure retries with backoff and recovers, and a
 * persistent transient failure eventually exhausts its retries and lands
 * on the DLQ. Each scenario is driven through the real producer → real
 * Redpanda → real consumer path, with only LEDGER itself stubbed (see
 * AbstractIntegrationTest).
 *
 * <p>Not covered here (see docs/RISKS.md): true broker-crash-then-redelivery
 * and true multi-partition ordering under concurrent consumption — both are
 * inherited guarantees from Kafka's documented protocol behavior and the
 * partitioning scheme (ADR-002), not independently re-proven by this suite,
 * the same way LEDGER's tests don't re-prove PostgreSQL's own ACID
 * guarantees.
 */
class TransactionEventFlowIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private EventProducer eventProducer;

    @Autowired
    private DlqCapture dlqCapture;

    @BeforeEach
    void clearDlqCapture() {
        dlqCapture.received.clear();
    }

    @Test
    void postsASuccessfulEventToLedgerAndNeverTouchesTheDlq() throws InterruptedException {
        ledgerStub.stubFor(post(urlEqualTo("/transactions"))
                .willReturn(aResponse()
                        .withStatus(201)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"id\":\"" + UUID.randomUUID() + "\"}")));

        eventProducer.publish(transferEvent("evt-success-1"));

        awaitUntil(() -> ledgerStub.getAllServeEvents().size() >= 1, Duration.ofSeconds(15));

        // Give the DLQ a moment to prove a negative — it should stay empty.
        DeadLetterEnvelope shouldBeNull = dlqCapture.received.poll(2, TimeUnit.SECONDS);
        assertThat(shouldBeNull).as("a successful post must never reach the DLQ").isNull();
        ledgerStub.verify(1, postRequestedFor(urlEqualTo("/transactions")));
    }

    @Test
    void routesAValidationRejectedEventToTheDlqWithoutRetrying() throws InterruptedException {
        ledgerStub.stubFor(post(urlEqualTo("/transactions"))
                .willReturn(aResponse()
                        .withStatus(400)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"error\":\"unbalanced_transaction\",\"message\":\"entries do not net to zero\"}")));

        eventProducer.publish(transferEvent("evt-rejected-1"));

        DeadLetterEnvelope envelope = dlqCapture.received.poll(15, TimeUnit.SECONDS);

        assertThat(envelope).as("a validation rejection must land on the DLQ").isNotNull();
        assertThat(envelope.event().eventId()).isEqualTo("evt-rejected-1");
        assertThat(envelope.reason()).contains("400");

        // No retry on a terminal (4xx) failure — exactly one call to LEDGER.
        ledgerStub.verify(1, postRequestedFor(urlEqualTo("/transactions")));
    }

    @Test
    void retriesATransientFailureAndEventuallySucceeds() throws InterruptedException {
        String scenario = "retry-then-success";
        ledgerStub.stubFor(post(urlEqualTo("/transactions"))
                .inScenario(scenario)
                .whenScenarioStateIs(STARTED)
                .willReturn(aResponse().withStatus(503))
                .willSetStateTo("recovered"));
        ledgerStub.stubFor(post(urlEqualTo("/transactions"))
                .inScenario(scenario)
                .whenScenarioStateIs("recovered")
                .willReturn(aResponse()
                        .withStatus(201)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"id\":\"" + UUID.randomUUID() + "\"}")));

        eventProducer.publish(transferEvent("evt-transient-recovers"));

        awaitUntil(() -> ledgerStub.getAllServeEvents().size() >= 2, Duration.ofSeconds(15));

        DeadLetterEnvelope shouldBeNull = dlqCapture.received.poll(2, TimeUnit.SECONDS);
        assertThat(shouldBeNull).as("a transient failure that later succeeds must never reach the DLQ").isNull();
        ledgerStub.verify(2, postRequestedFor(urlEqualTo("/transactions")));
    }

    @Test
    void exhaustsRetriesAndRoutesToTheDlqOnPersistentTransientFailure() throws InterruptedException {
        ledgerStub.stubFor(post(urlEqualTo("/transactions")).willReturn(aResponse().withStatus(503)));

        eventProducer.publish(transferEvent("evt-persistently-down"));

        // Default policy: 1 initial attempt + 3 retries = 4 calls before the DLQ.
        DeadLetterEnvelope envelope = dlqCapture.received.poll(15, TimeUnit.SECONDS);

        assertThat(envelope).as("exhausted retries must land on the DLQ").isNotNull();
        assertThat(envelope.event().eventId()).isEqualTo("evt-persistently-down");
        assertThat(envelope.reason()).contains("exhausted retries");
        ledgerStub.verify(4, postRequestedFor(urlEqualTo("/transactions")));
    }

    private WireTransactionEvent transferEvent(String eventId) {
        UUID cash = UUID.randomUUID();
        UUID revenue = UUID.randomUUID();
        return new WireTransactionEvent(
                eventId,
                "integration test transfer",
                List.of(
                        new WireEntry(cash, new BigDecimal("10.00"), "USD"),
                        new WireEntry(revenue, new BigDecimal("-10.00"), "USD")),
                Instant.now());
    }

    private void awaitUntil(java.util.function.Supplier<Boolean> condition, Duration timeout) {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (Boolean.TRUE.equals(condition.get())) {
                return;
            }
            sleep(Duration.ofMillis(100));
        }
        throw new AssertionError("condition not met within " + timeout);
    }

    private void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
