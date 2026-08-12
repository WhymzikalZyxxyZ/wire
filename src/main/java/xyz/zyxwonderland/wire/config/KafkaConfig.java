package xyz.zyxwonderland.wire.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import xyz.zyxwonderland.wire.dlq.DeadLetterEnvelope;
import xyz.zyxwonderland.wire.event.WireTransactionEvent;

/**
 * Producer/consumer factories are defined explicitly here, rather than
 * left to Spring Boot's Kafka autoconfiguration, so the JsonSerializer and
 * JsonDeserializer share the application's own Jackson ObjectMapper
 * (needed for Instant/BigDecimal support via the jsr310 module already
 * registered by Spring Boot) instead of each constructing their own.
 */
@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    /**
     * Without an explicit partition count, the broker creates
     * wire.transactions.raw with its own default (often 1) — which would
     * silently defeat ADR-002's whole partition-key ordering scheme, since
     * a single partition makes the key irrelevant. Declaring it here (via
     * Spring Kafka's KafkaAdmin auto-provisioning) makes the multi-partition
     * topology this design depends on an explicit, checked-in fact instead
     * of an unstated assumption. See docs/RISKS.md.
     */
    @Bean
    public NewTopic rawTopic(
            @Value("${wire.topics.raw}") String rawTopic,
            @Value("${wire.topics.raw-partitions:3}") int partitions) {
        return TopicBuilder.name(rawTopic).partitions(partitions).replicas(1).build();
    }

    @Bean
    public NewTopic dlqTopic(
            @Value("${wire.topics.dlq}") String dlqTopic,
            @Value("${wire.topics.dlq-partitions:1}") int partitions) {
        return TopicBuilder.name(dlqTopic).partitions(partitions).replicas(1).build();
    }

    @Bean
    public ProducerFactory<String, WireTransactionEvent> producerFactory(ObjectMapper objectMapper) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        JsonSerializer<WireTransactionEvent> valueSerializer = new JsonSerializer<>(objectMapper);
        valueSerializer.setAddTypeInfo(false);
        return new DefaultKafkaProducerFactory<>(props, new StringSerializer(), valueSerializer);
    }

    @Bean
    public KafkaTemplate<String, WireTransactionEvent> kafkaTemplate(
            ProducerFactory<String, WireTransactionEvent> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    public ConsumerFactory<String, WireTransactionEvent> consumerFactory(
            ObjectMapper objectMapper,
            @Value("${spring.kafka.consumer.group-id}") String groupId) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // ADR-002: the offset only advances after a terminal outcome
        // (LEDGER success, or a DLQ publish) — never on a timer.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        JsonDeserializer<WireTransactionEvent> valueDeserializer =
                new JsonDeserializer<>(WireTransactionEvent.class, objectMapper).trustedPackages("*");
        valueDeserializer.setUseTypeMapperForKey(false);
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), valueDeserializer);
    }

    @Bean
    public ProducerFactory<String, DeadLetterEnvelope> deadLetterProducerFactory(ObjectMapper objectMapper) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        JsonSerializer<DeadLetterEnvelope> valueSerializer = new JsonSerializer<>(objectMapper);
        valueSerializer.setAddTypeInfo(false);
        return new DefaultKafkaProducerFactory<>(props, new StringSerializer(), valueSerializer);
    }

    @Bean
    public KafkaTemplate<String, DeadLetterEnvelope> deadLetterKafkaTemplate(
            ProducerFactory<String, DeadLetterEnvelope> deadLetterProducerFactory) {
        return new KafkaTemplate<>(deadLetterProducerFactory);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, WireTransactionEvent> kafkaListenerContainerFactory(
            ConsumerFactory<String, WireTransactionEvent> consumerFactory,
            @Value("${wire.consumer.concurrency:3}") int concurrency) {
        ConcurrentKafkaListenerContainerFactory<String, WireTransactionEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        // Matches wire.topics.raw-partitions — one thread per partition, so
        // concurrent multi-partition consumption is actually exercised, not
        // just structurally possible. See docs/RISKS.md.
        factory.setConcurrency(concurrency);
        return factory;
    }
}
