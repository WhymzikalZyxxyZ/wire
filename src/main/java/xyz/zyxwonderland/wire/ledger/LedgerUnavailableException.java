package xyz.zyxwonderland.wire.ledger;

/**
 * A network error, timeout, or 5xx from LEDGER — transient, per
 * docs/adr/003-failure-handling.md. The same request will likely succeed
 * on a later attempt, so the caller retries with backoff before ever
 * considering the DLQ.
 */
public class LedgerUnavailableException extends RuntimeException {

    public LedgerUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
