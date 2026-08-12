package xyz.zyxwonderland.wire;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import xyz.zyxwonderland.wire.dlq.DeadLetterEnvelope;

/**
 * Test-only consumer of wire.transactions.dlq — lets tests assert on what
 * actually landed on the DLQ instead of only on what didn't crash. Lives
 * under src/test, so it never ships in the application itself. The
 * listener container factory it depends on is defined separately in
 * {@link DlqTestConfig} — see that class's Javadoc for why.
 */
@Component
public class DlqCapture {

    public final BlockingQueue<DeadLetterEnvelope> received = new LinkedBlockingQueue<>();

    @KafkaListener(topics = "${wire.topics.dlq}", containerFactory = "dlqListenerContainerFactory")
    public void onDeadLetter(DeadLetterEnvelope envelope) {
        received.add(envelope);
    }
}
