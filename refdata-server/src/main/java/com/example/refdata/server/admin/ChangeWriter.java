package com.example.refdata.server.admin;

import com.example.refdata.model.Account;
import com.example.refdata.model.Address;
import com.example.refdata.model.ChangeBatch;
import com.example.refdata.model.Client;
import com.example.refdata.model.CodedEnum;
import com.example.refdata.model.Country;
import com.example.refdata.model.Currency;
import com.example.refdata.model.EntityKind;
import com.example.refdata.model.SettlementInstruction;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Writes a {@link ChangeBatch} to the reference data tables in one database transaction, together with its outbox row
 * ({@code REFDATA_OUTBOX}) and a {@code PUBLISHED} release row for the new data version. Either everything commits or
 * nothing does; the outbox row is what {@link OutboxRelay} later publishes, so a crash between commit and publication
 * loses nothing.
 * <p>Column mapping mirrors the JDBC loader in reverse: enums as their database codes, epoch days as DATE (open-ended
 * sentinels as NULL), epoch milliseconds as a UTC TIMESTAMP, {@code NO_PARENT} as NULL.
 */
final class ChangeWriter {
    static final String OUTBOX_TABLE = "REFDATA_OUTBOX";
    /** Derby's SQLState for "table already exists". */
    private static final String TABLE_EXISTS = "X0Y32";

    /** One column: its name and how to read its SQL value from a record. */
    private record Column(String name, Function<Object, Object> value) {}

    /** The columns of one table; the first is the primary key. */
    private record Table(String name, List<Column> columns) {
        Column key() { return columns.get(0); }
        List<Column> nonKey() { return columns.subList(1, columns.size()); }
    }

    private final ConnectionFactory connections;
    private final Calendar utc = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

    ChangeWriter(ConnectionFactory connections) {
        this.connections = connections;
    }

    /** Creates the outbox table unless it exists. */
    void ensureOutboxTable() throws SQLException {
        try (Connection c = connections.open(); Statement s = c.createStatement()) {
            s.executeUpdate("CREATE TABLE " + OUTBOX_TABLE + " (version BIGINT PRIMARY KEY, payload BLOB(" + com.example.refdata.contract.ChangeCodec.MAX_BATCH_BYTES + ") NOT NULL)");
        } catch (SQLException e) {
            if (!TABLE_EXISTS.equals(e.getSQLState())) throw e;
        }
    }

    /**
     * Writes the rows, the outbox entry and the release row in one transaction; rolls back on any failure.
     * Changes are written in the order given, so the caller is responsible for a valid dependency order
     * (parents inserted before children, children deleted before parents).
     */
    void write(ChangeBatch batch, byte[] encodedBatch) throws SQLException {
        try (Connection c = connections.open()) {
            c.setAutoCommit(false);
            try {
                for (ChangeBatch.Change change : batch.changes()) write(c, change);
                try (PreparedStatement s = c.prepareStatement("INSERT INTO " + OUTBOX_TABLE + "(version, payload) VALUES (?, ?)")) {
                    s.setLong(1, batch.version());
                    s.setBytes(2, encodedBatch);
                    s.executeUpdate();
                }
                try (PreparedStatement s = c.prepareStatement("INSERT INTO REFDATA_RELEASE(release_id, status, published_at, description) "
                        + "VALUES (?, 'PUBLISHED', CURRENT_TIMESTAMP, 'live transaction')")) {
                    s.setLong(1, batch.version());
                    s.executeUpdate();
                }
                c.commit();
            } catch (SQLException | RuntimeException e) {
                try { c.rollback(); } catch (SQLException suppressed) { e.addSuppressed(suppressed); }
                throw e;
            }
        }
    }

    private void write(Connection c, ChangeBatch.Change change) throws SQLException {
        Table table = table(change.kind());
        String key = table.key().name();
        Object keyValue = change.kind().hasCodeKey() ? change.key() : Long.valueOf(change.key());
        switch (change.operation()) {
            case DELETE -> {
                try (PreparedStatement s = c.prepareStatement("DELETE FROM " + table.name() + " WHERE " + key + " = ?")) {
                    s.setObject(1, keyValue);
                    expectOneRow(s.executeUpdate(), change);
                }
            }
            case INSERT -> {
                String names = table.columns().stream().map(Column::name).collect(Collectors.joining(", "));
                String marks = String.join(", ", Collections.nCopies(table.columns().size(), "?"));
                try (PreparedStatement s = c.prepareStatement("INSERT INTO " + table.name() + " (" + names + ") VALUES (" + marks + ")")) {
                    int index = 1;
                    for (Column col : table.columns()) bind(s, index++, col.value().apply(change.record()));
                    expectOneRow(s.executeUpdate(), change);
                }
            }
            case UPDATE -> {
                String sets = table.nonKey().stream().map(col -> col.name() + " = ?").collect(Collectors.joining(", "));
                try (PreparedStatement s = c.prepareStatement("UPDATE " + table.name() + " SET " + sets + " WHERE " + key + " = ?")) {
                    int index = 1;
                    for (Column col : table.nonKey()) bind(s, index++, col.value().apply(change.record()));
                    s.setObject(index, keyValue);
                    expectOneRow(s.executeUpdate(), change);
                }
            }
        }
    }

    private void bind(PreparedStatement s, int index, Object value) throws SQLException {
        if (value instanceof Timestamp ts) s.setTimestamp(index, ts, utc);
        else s.setObject(index, value);
    }

    private static void expectOneRow(int rows, ChangeBatch.Change change) throws SQLException {
        if (rows != 1) throw new SQLException(change.operation() + " of " + change.kind() + ":" + change.key() + " affected " + rows + " rows");
    }

    // ------------------------------------------------------------------ column mapping per table

    private static final Table COUNTRY = new Table("COUNTRY", List.of(
            col("country_code", (Country r) -> r.code()),
            col("iso3", (Country r) -> r.iso3()),
            col("numeric_code", (Country r) -> r.numericCode()),
            col("name", (Country r) -> r.name()),
            col("region", (Country r) -> emptyIfNull(r.region()))));

    private static final Table CURRENCY = new Table("CURRENCY", List.of(
            col("currency_code", (Currency r) -> r.code()),
            col("numeric_code", (Currency r) -> r.numericCode()),
            col("name", (Currency r) -> r.name()),
            col("minor_units", (Currency r) -> r.minorUnits()),
            col("active", (Currency r) -> r.active())));

    private static final Table CLIENT = new Table("CLIENT", List.of(
            col("client_id", (Client r) -> r.id()),
            col("client_ref", (Client r) -> r.ref()),
            col("lei", (Client r) -> r.lei()),
            col("legal_name", (Client r) -> r.legalName()),
            col("short_name", (Client r) -> emptyIfNull(r.shortName())),
            col("client_type", (Client r) -> code(r.type())),
            col("status", (Client r) -> code(r.status())),
            col("incorporation_country", (Client r) -> r.incorporationCountry()),
            col("parent_client_id", (Client r) -> r.hasParent() ? r.parentClientId() : null),
            col("risk_rating", (Client r) -> r.riskRating()),
            col("onboarded_date", (Client r) -> date(r.onboardedDate())),
            col("row_version", (Client r) -> r.rowVersion()),
            col("updated_at", (Client r) -> new Timestamp(r.updatedAt()))));

    private static final Table ADDRESS = new Table("ADDRESS", List.of(
            col("address_id", (Address r) -> r.id()),
            col("client_id", (Address r) -> r.clientId()),
            col("address_type", (Address r) -> code(r.type())),
            col("line1", (Address r) -> r.line1()),
            col("line2", (Address r) -> r.line2()),
            col("line3", (Address r) -> r.line3()),
            col("city", (Address r) -> r.city()),
            col("region", (Address r) -> r.region()),
            col("postal_code", (Address r) -> r.postalCode()),
            col("country_code", (Address r) -> r.countryCode()),
            col("is_primary", (Address r) -> r.primary()),
            col("valid_from", (Address r) -> date(r.validFrom())),
            col("valid_to", (Address r) -> date(r.validTo())),
            col("row_version", (Address r) -> r.rowVersion()),
            col("updated_at", (Address r) -> new Timestamp(r.updatedAt()))));

    private static final Table ACCOUNT = new Table("ACCOUNT", List.of(
            col("account_id", (Account r) -> r.id()),
            col("account_number", (Account r) -> r.number()),
            col("client_id", (Account r) -> r.clientId()),
            col("account_type", (Account r) -> code(r.type())),
            col("base_currency", (Account r) -> r.baseCurrency()),
            col("booking_country", (Account r) -> r.bookingCountry()),
            col("status", (Account r) -> code(r.status())),
            col("opened_date", (Account r) -> date(r.openedDate())),
            col("closed_date", (Account r) -> date(r.closedDate())),
            col("row_version", (Account r) -> r.rowVersion()),
            col("updated_at", (Account r) -> new Timestamp(r.updatedAt()))));

    private static final Table SETTLEMENT_INSTRUCTION = new Table("SETTLEMENT_INSTRUCTION", List.of(
            col("ssi_id", (SettlementInstruction r) -> r.id()),
            col("account_id", (SettlementInstruction r) -> r.accountId()),
            col("currency_code", (SettlementInstruction r) -> r.currencyCode()),
            col("asset_class", (SettlementInstruction r) -> code(r.assetClass())),
            col("beneficiary_bic", (SettlementInstruction r) -> r.beneficiaryBic()),
            col("agent_bic", (SettlementInstruction r) -> r.agentBic()),
            col("agent_account", (SettlementInstruction r) -> r.agentAccount()),
            col("is_default", (SettlementInstruction r) -> r.isDefault()),
            col("effective_from", (SettlementInstruction r) -> date(r.effectiveFrom())),
            col("effective_to", (SettlementInstruction r) -> date(r.effectiveTo())),
            col("row_version", (SettlementInstruction r) -> r.rowVersion()),
            col("updated_at", (SettlementInstruction r) -> new Timestamp(r.updatedAt()))));

    private static Table table(EntityKind kind) {
        return switch (kind) {
            case COUNTRY -> COUNTRY;
            case CURRENCY -> CURRENCY;
            case CLIENT -> CLIENT;
            case ADDRESS -> ADDRESS;
            case ACCOUNT -> ACCOUNT;
            case SETTLEMENT_INSTRUCTION -> SETTLEMENT_INSTRUCTION;
        };
    }

    @SuppressWarnings("unchecked")
    private static <R> Column col(String name, Function<R, Object> value) {
        return new Column(name, row -> value.apply((R) row));
    }

    /** Epoch day as a DATE; the open-ended / not-closed sentinel ({@link Integer#MAX_VALUE}) as NULL. */
    private static java.sql.Date date(int epochDay) {
        return epochDay == Integer.MAX_VALUE || epochDay == Integer.MIN_VALUE ? null : java.sql.Date.valueOf(LocalDate.ofEpochDay(epochDay));
    }

    private static String code(CodedEnum value) { return value == null ? null : value.dbCode(); }

    /** The schema keeps these optional text columns NOT NULL, with "" for absent (the codec maps "" back to null). */
    private static String emptyIfNull(String s) { return s == null ? "" : s; }
}
