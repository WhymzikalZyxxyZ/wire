package xyz.zyxwonderland.wire.producer;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import xyz.zyxwonderland.wire.event.WireTransactionEvent;

/**
 * The producer side of docs/architecture/overview.md's flow — publishes a
 * whole balanced transaction as one message, keyed by
 * {@link WireTransactionEvent#partitionKey()} for per-account ordering
 * (ADR-002). Waits for broker acknowledgment before returning, so a
 * caller that gets a successful response knows the event is durably
 * queued, not just handed to a client-side buffer.
 */
@Component
public class EventProducer {

    private final KafkaTemplate<String, WireTransactionEvent> kafkaTemplate;
    private final String rawTopic;

    public EventProducer(
            KafkaTemplate<String, WireTransactionEvent> kafkaTemplate,
            @Value("${wire.topics.raw}") String rawTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.rawTopic = rawTopic;
    }

    /** @throws EventPublishException if the broker can't be reached — see EventController's handling. */
    public void publish(WireTransactionEvent event) {
        try {
            kafkaTemplate.send(rawTopic, event.partitionKey().toString(), event).get(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EventPublishException("interrupted publishing event " + event.eventId(), e);
        } catch (ExecutionException | TimeoutException e) {
            throw new EventPublishException("failed to publish event " + event.eventId(), e);
        }
    }
}
