package xyz.zyxwonderland.wire.ledger;

import java.util.UUID;

public record LedgerResult(UUID transactionId, boolean created) {
}
