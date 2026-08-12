package xyz.zyxwonderland.wire.event;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.math.BigDecimal;
import java.util.UUID;

/** Positive amount = debit, negative = credit — mirrors LEDGER's EntryRequest exactly. */
public record WireEntry(
        @NotNull UUID accountId,
        @NotNull BigDecimal amount,
        @NotNull @Size(min = 3, max = 3) String currency
) {
}
