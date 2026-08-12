package xyz.zyxwonderland.wire.ledger;

import java.util.List;

/** Mirrors LEDGER's own SubmitTransactionRequest DTO exactly. */
public record LedgerTransactionRequest(String idempotencyKey, String description, List<LedgerEntryRequest> entries) {
}
