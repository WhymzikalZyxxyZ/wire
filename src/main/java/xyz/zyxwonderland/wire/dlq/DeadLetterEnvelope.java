package xyz.zyxwonderland.wire.dlq;

import java.time.Instant;
import xyz.zyxwonderland.wire.event.WireTransactionEvent;

/** What actually lands on wire.transactions.dlq — the event plus why it failed. */
public record DeadLetterEnvelope(WireTransactionEvent event, String reason, Instant failedAt) {
}
