package xyz.zyxwonderland.wire.ledger;

import java.util.UUID;

/**
 * Only the field WIRE actually needs from LEDGER's TransactionResponse.
 * Extra fields in the real response (idempotencyKey, status, entries, ...)
 * are ignored on deserialization rather than requiring an exact mirror.
 */
public record LedgerTransactionResponse(UUID id) {
}
