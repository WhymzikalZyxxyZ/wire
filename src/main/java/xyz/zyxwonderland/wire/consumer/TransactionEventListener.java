package xyz.zyxwonderland.wire.consumer;

import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import xyz.zyxwonderland.wire.dlq.DeadLetterPublisher;
import xyz.zyxwonderland.wire.event.WireTransactionEvent;
import xyz.zyxwonderland.wire.ledger.LedgerClient;
import xyz.zyxwonderland.wire.ledger.LedgerResult;
import xyz.zyxwonderland.wire.ledger.LedgerUnavailableException;
import xyz.zyxwonderland.wire.ledger.LedgerValidationException;

/**
 * The consumer side of docs/architecture/overview.md's flow. Manual
 * offset commit (see KafkaConfig's ENABLE_AUTO_COMMIT_CONFIG=false) means
 * {@link #onMessage} only acknowledges after {@link #process} reaches a
 * terminal outcome — a successful/idempotent LEDGER call or a durably
 * published DLQ record — never on a timer. A crash anywhere before that
 * point causes the broker to redeliver the event on restart, which is
 * safe by construction (ADR-002): LEDGER's idempotency key makes a repeat
 * call a no-op, and a repeat DLQ publish is a harmless duplicate record.
 */
@Component
public class TransactionEventListener {

    private static final Logger log = LoggerFactory.getLogger(TransactionEventListener.class);

    private final LedgerClient ledgerClient;
    private final DeadLetterPublisher deadLetterPublisher;
    private final RetryPolicy retryPolicy;

    public TransactionEventListener(
            LedgerClient ledgerClient, DeadLetterPublisher deadLetterPublisher, RetryPolicy retryPolicy) {
        this.ledgerClient = ledgerClient;
        this.deadLetterPublisher = deadLetterPublisher;
        this.retryPolicy = retryPolicy;
    }

    @KafkaListener(topics = "${wire.topics.raw}", containerFactory = "kafkaListenerContainerFactory")
    public void onMessage(WireTransactionEvent event, Acknowledgment acknowledgment) {
        process(event);
        acknowledgment.acknowledge();
    }

    private void process(WireTransactionEvent event) {
        int attempt = 0;
        while (true) {
            try {
                LedgerResult result = ledgerClient.submit(event);
                log.info(
                        "event {} -> LEDGER transaction {} (created={})",
                        event.eventId(), result.transactionId(), result.created());
                return;
            } catch (LedgerValidationException e) {
                log.warn("event {} rejected by LEDGER, routing to DLQ: {}", event.eventId(), e.getMessage());
                deadLetterPublisher.publish(event, e.getMessage());
                return;
            } catch (LedgerUnavailableException e) {
                attempt++;
                if (attempt > retryPolicy.maxAttempts()) {
                    log.warn(
                            "event {} exhausted {} retries, routing to DLQ",
                            event.eventId(), retryPolicy.maxAttempts());
                    deadLetterPublisher.publish(event, "exhausted retries: " + e.getMessage());
                    return;
                }
                Duration backoff = retryPolicy.backoffFor(attempt);
                log.info(
                        "event {} transient failure (attempt {}/{}), retrying in {}",
                        event.eventId(), attempt, retryPolicy.maxAttempts(), backoff);
                sleep(backoff);
            }
        }
    }

    private void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while backing off", ie);
        }
    }
}
