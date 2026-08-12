package xyz.zyxwonderland.wire.event;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * One event = one already-balanced transaction (all its entries together),
 * not one entry per message — see docs/architecture/overview.md. This
 * avoids needing a stateful correlator to reassemble entries produced as
 * separate messages, at the cost of assuming upstream sources emit whole
 * transactions rather than individual legs; that tradeoff is named in
 * docs/RISKS.md, not hidden.
 */
public record WireTransactionEvent(
        @NotBlank String eventId,
        String description,
        @NotEmpty @Valid List<WireEntry> entries,
        Instant occurredAt
) {

    /**
     * The account used as the Kafka partition key — see
     * docs/adr/002-delivery-and-consistency.md's Notes. Every entry's
     * account within one event is guaranteed co-located (they arrive in one
     * message), but two transactions sharing an account only land on the
     * same partition if they happen to pick the same canonical entry; true
     * global per-account ordering across all of an account's transactions
     * is not structurally guaranteed by this scheme, and is named as an
     * accepted limitation in docs/RISKS.md.
     */
    public UUID partitionKey() {
        return entries.get(0).accountId();
    }
}
