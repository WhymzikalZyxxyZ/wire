package xyz.zyxwonderland.wire.ledger;

/**
 * LEDGER rejected the transaction outright (4xx) — terminal, per
 * docs/adr/003-failure-handling.md. Retrying an identical request would
 * fail identically every time, so the caller routes straight to the DLQ
 * instead of retrying.
 */
public class LedgerValidationException extends RuntimeException {

    public LedgerValidationException(String message) {
        super(message);
    }

    public LedgerValidationException(String message, Throwable cause) {
        super(message, cause);
    }
}
