package xyz.zyxwonderland.wire.ledger;

import java.math.BigDecimal;
import java.util.UUID;

/** Mirrors LEDGER's own EntryRequest DTO exactly — see ledger/src/main/java/.../api/EntryRequest.java. */
public record LedgerEntryRequest(UUID accountId, BigDecimal amount, String currency) {
}
