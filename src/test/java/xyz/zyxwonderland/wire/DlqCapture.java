package xyz.zyxwonderland.wire;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Component;
import xyz.zyxwonderland.wire.dlq.DeadLetterEnvelope;

/**
 * Test-only consumer of wire.transactions.dlq — lets tests assert on what
 * actually landed on the DLQ instead of only on what didn't crash. Lives
 * under src/test, so it never ships in the application itself.
 */
@Component
public class DlqCapture {

    public final BlockingQueue<DeadLetterEnvelope> received = new LinkedBlockingQueue<>();

    @Bean
    public ConsumerFactory<String, DeadLetterEnvelope> dlqConsumerFactory(
            ObjectMapper objectMapper, @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-dlq-capture");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        JsonDeserializer<DeadLetterEnvelope> deserializer =
                new JsonDeserializer<>(DeadLetterEnvelope.class, objectMapper).trustedPackages("*");
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), deserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, DeadLetterEnvelope> dlqListenerContainerFactory(
            ConsumerFactory<String, DeadLetterEnvelope> dlqConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, DeadLetterEnvelope> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(dlqConsumerFactory);
        return factory;
    }

    @KafkaListener(topics = "${wire.topics.dlq}", containerFactory = "dlqListenerContainerFactory")
    public void onDeadLetter(DeadLetterEnvelope envelope) {
        received.add(envelope);
    }
}
