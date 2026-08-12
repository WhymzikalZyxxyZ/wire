package xyz.zyxwonderland.wire;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Base class for tests that need a real Kafka-protocol broker and a
 * stubbed LEDGER HTTP boundary — see
 * docs/adr/004-correctness-verification.md.
 *
 * <p>Both are started once in a static initializer (the singleton
 * pattern), not managed via {@code @Container}/{@code @Testcontainers}:
 * LEDGER's own test suite hit a real bug where a per-test-class-managed
 * static container field was torn down after the first test class
 * finished, leaving every later test class talking to a dead container.
 * Starting both here once and never stopping them (the JVM reaps them at
 * process exit) avoids that entirely.
 *
 * <p>Properties are wired via {@code @DynamicPropertySource} rather than
 * {@code @ServiceConnection}: WIRE's {@code KafkaConfig} defines its own
 * {@code ProducerFactory}/{@code ConsumerFactory} beans directly from the
 * {@code spring.kafka.bootstrap-servers} property (so the JSON
 * (de)serializers can share the app's Jackson {@code ObjectMapper}), which
 * bypasses the {@code KafkaConnectionDetails} bean {@code @ServiceConnection}
 * would populate — so the property itself has to be set directly instead.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public abstract class AbstractIntegrationTest {

    static final RedpandaContainer redpanda =
            new RedpandaContainer(DockerImageName.parse("redpandadata/redpanda:v24.2.4"));

    public static final WireMockServer ledgerStub = new WireMockServer(WireMockConfiguration.options().dynamicPort());

    static {
        redpanda.start();
        ledgerStub.start();
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", redpanda::getBootstrapServers);
        registry.add("wire.ledger.base-url", () -> "http://localhost:" + ledgerStub.port());
    }

    @BeforeEach
    void resetLedgerStub() {
        ledgerStub.resetAll();
    }
}
