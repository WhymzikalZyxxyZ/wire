package xyz.zyxwonderland.wire.event;

import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import java.math.BigDecimal;
import java.util.UUID;

/**
 * Positive amount = debit, negative = credit — mirrors LEDGER's
 * EntryRequest exactly, including the bounds a security audit found
 * missing there and here: {@code @Digits} matches LEDGER's
 * NUMERIC(19,4) column so an over-precision amount is rejected here
 * (400) instead of only failing once it reaches LEDGER, and
 * {@code @Pattern} rejects non-uppercase-ISO currency codes.
 */
public record WireEntry(
        @NotNull UUID accountId,
        @NotNull @Digits(integer = 15, fraction = 4) BigDecimal amount,
        @NotNull @Size(min = 3, max = 3) @Pattern(regexp = "^[A-Z]{3}$") String currency
) {
}
