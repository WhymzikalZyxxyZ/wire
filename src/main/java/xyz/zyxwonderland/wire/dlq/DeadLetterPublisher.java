package xyz.zyxwonderland.wire.dlq;

import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import xyz.zyxwonderland.wire.event.WireTransactionEvent;

/**
 * Publishes to wire.transactions.dlq and waits for broker acknowledgment
 * before returning — the caller (see TransactionEventListener) must only
 * commit the original offset once the DLQ record is durably written, or a
 * crash between "send() called" and actual delivery would silently drop a
 * failure record instead of just losing an event, which is worse.
 */
@Component
public class DeadLetterPublisher {

    private final KafkaTemplate<String, DeadLetterEnvelope> kafkaTemplate;
    private final String dlqTopic;

    public DeadLetterPublisher(
            KafkaTemplate<String, DeadLetterEnvelope> kafkaTemplate,
            @Value("${wire.topics.dlq}") String dlqTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.dlqTopic = dlqTopic;
    }

    /** @throws DeadLetterPublishException if the DLQ itself can't be reached — see TransactionEventListener. */
    public void publish(WireTransactionEvent event, String reason) {
        var envelope = new DeadLetterEnvelope(event, reason, Instant.now());
        try {
            kafkaTemplate.send(dlqTopic, event.partitionKey().toString(), envelope).get(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DeadLetterPublishException("interrupted publishing to DLQ for event " + event.eventId(), e);
        } catch (ExecutionException | TimeoutException e) {
            throw new DeadLetterPublishException("failed to publish to DLQ for event " + event.eventId(), e);
        }
    }
}
