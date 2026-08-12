package xyz.zyxwonderland.wire.consumer;

import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/** Bounded retry with exponential backoff for transient LEDGER failures — see ADR-003. */
@Component
public class RetryPolicy {

    private final int maxAttempts;
    private final Duration initialBackoff;

    public RetryPolicy(
            @Value("${wire.retry.max-attempts:3}") int maxAttempts,
            @Value("${wire.retry.initial-backoff-ms:200}") long initialBackoffMs) {
        this.maxAttempts = maxAttempts;
        this.initialBackoff = Duration.ofMillis(initialBackoffMs);
    }

    public int maxAttempts() {
        return maxAttempts;
    }

    /** attempt 1 → initialBackoff, attempt 2 → 2x, attempt 3 → 4x, ... */
    public Duration backoffFor(int attempt) {
        return initialBackoff.multipliedBy(1L << (attempt - 1));
    }
}
