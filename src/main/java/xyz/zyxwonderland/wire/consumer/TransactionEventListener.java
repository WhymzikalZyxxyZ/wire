package xyz.zyxwonderland.wire.consumer;

import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import xyz.zyxwonderland.wire.dlq.DeadLetterPublishException;
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
                publishToDeadLetterOrEscalate(event, e.getMessage());
                return;
            } catch (LedgerUnavailableException e) {
                attempt++;
                if (attempt > retryPolicy.maxAttempts()) {
                    log.warn(
                            "event {} exhausted {} retries, routing to DLQ",
                            event.eventId(), retryPolicy.maxAttempts());
                    publishToDeadLetterOrEscalate(event, "exhausted retries: " + e.getMessage());
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

    /**
     * The DLQ is meant to be the last resort — but if the DLQ publish
     * itself fails (found in a post-implementation code survey: previously
     * an uncaught exception here would skip {@code acknowledgment.acknowledge()}
     * entirely, causing the broker to redeliver this event forever and
     * blocking the partition indefinitely, the exact head-of-line-blocking
     * failure mode ADR-003 exists to prevent — just one layer deeper than
     * it was designed for), there's no further durable fallback to reach
     * for. Logging at ERROR and letting the caller still acknowledge trades
     * guaranteed DLQ durability for avoiding that infinite-redelivery
     * lock-up in this rare double-failure case (LEDGER *and* the broker's
     * DLQ path both unreachable at once). A real production system would
     * want a true last-resort sink (local disk, a paging alert) here —
     * named as an accepted gap in docs/RISKS.md, not a hidden one.
     */
    private void publishToDeadLetterOrEscalate(WireTransactionEvent event, String reason) {
        try {
            deadLetterPublisher.publish(event, reason);
        } catch (DeadLetterPublishException e) {
            log.error(
                    "CRITICAL: event {} failed at LEDGER ({}) AND failed to publish to the DLQ ({}); "
                            + "acknowledging anyway to avoid blocking this partition forever — "
                            + "this event's failure is recorded only in this log line, manual recovery required",
                    event.eventId(), reason, e.getMessage(), e);
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
