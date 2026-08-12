package xyz.zyxwonderland.wire;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import xyz.zyxwonderland.wire.dlq.DeadLetterEnvelope;

/**
 * Split out from {@link DlqCapture} deliberately: a {@code @Component}
 * that both owns {@code @Bean} factory methods AND a {@code @KafkaListener}
 * that depends on one of those factories creates a genuine circular
 * dependency (Spring must fully construct the component instance before it
 * can call its own instance-level {@code @Bean} method, but listener
 * registration on that same instance needs the factory bean first). Moving
 * the factories into their own {@code @Configuration} class breaks the
 * cycle.
 */
@Configuration
public class DlqTestConfig {

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
}
