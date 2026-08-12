package xyz.zyxwonderland.wire.dlq;

/**
 * The DLQ publish itself failed — the last-resort failure path has no
 * further fallback of its own. See {@link DeadLetterPublisher} and
 * {@code TransactionEventListener}'s handling of this exception for the
 * accepted tradeoff this forces (docs/RISKS.md).
 */
public class DeadLetterPublishException extends RuntimeException {

    public DeadLetterPublishException(String message, Throwable cause) {
        super(message, cause);
    }
}
