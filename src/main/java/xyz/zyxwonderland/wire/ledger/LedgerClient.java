package xyz.zyxwonderland.wire.ledger;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;
import xyz.zyxwonderland.wire.event.WireTransactionEvent;

/**
 * The only thing standing between WIRE and LEDGER. Maps a 4xx response to
 * {@link LedgerValidationException} (terminal — see ADR-003) and a 5xx,
 * timeout, or connection failure to {@link LedgerUnavailableException}
 * (transient — retry). {@code event.eventId()} is passed to LEDGER
 * verbatim as {@code idempotencyKey} — see ADR-002's Notes for why the
 * field is renamed at this exact boundary.
 *
 * <p>Explicit connect/read timeouts (found missing in a post-implementation
 * code survey — see docs/RISKS.md) are what actually make a hung LEDGER
 * instance surface as {@link ResourceAccessException} at all: without
 * them, a connection that never closes would block the consumer thread
 * indefinitely instead of ever reaching the transient-failure retry path.
 */
@Component
public class LedgerClient {

    private final RestClient restClient;

    public LedgerClient(
            RestClient.Builder builder,
            @Value("${wire.ledger.base-url}") String baseUrl,
            @Value("${wire.ledger.connect-timeout-ms:5000}") int connectTimeoutMs,
            @Value("${wire.ledger.read-timeout-ms:10000}") int readTimeoutMs) {
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(connectTimeoutMs);
        requestFactory.setReadTimeout(readTimeoutMs);
        this.restClient = builder.baseUrl(baseUrl).requestFactory(requestFactory).build();
    }

    public LedgerResult submit(WireTransactionEvent event) {
        var entries = event.entries().stream()
                .map(e -> new LedgerEntryRequest(e.accountId(), e.amount(), e.currency()))
                .toList();
        var request = new LedgerTransactionRequest(event.eventId(), event.description(), entries);

        try {
            ResponseEntity<LedgerTransactionResponse> response = restClient.post()
                    .uri("/transactions")
                    .body(request)
                    .retrieve()
                    .toEntity(LedgerTransactionResponse.class);

            LedgerTransactionResponse body = response.getBody();
            if (body == null || body.id() == null) {
                throw new LedgerUnavailableException(
                        "LEDGER returned an empty body for event " + event.eventId(), null);
            }
            boolean created = response.getStatusCode().value() == 201;
            return new LedgerResult(body.id(), created);
        } catch (HttpClientErrorException e) {
            throw new LedgerValidationException(
                    "LEDGER rejected event " + event.eventId() + ": " + e.getStatusCode() + " "
                            + e.getResponseBodyAsString(),
                    e);
        } catch (HttpServerErrorException | ResourceAccessException e) {
            throw new LedgerUnavailableException("LEDGER unreachable for event " + event.eventId(), e);
        }
    }
}
