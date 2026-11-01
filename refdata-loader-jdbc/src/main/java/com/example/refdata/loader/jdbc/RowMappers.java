package com.example.refdata.loader.jdbc;

import com.example.refdata.model.Account;
import com.example.refdata.model.AccountType;
import com.example.refdata.model.Address;
import com.example.refdata.model.AddressType;
import com.example.refdata.model.AssetClass;
import com.example.refdata.model.Client;
import com.example.refdata.model.ClientType;
import com.example.refdata.model.Country;
import com.example.refdata.model.Currency;
import com.example.refdata.model.EntityKind;
import com.example.refdata.model.RefDataSink;
import com.example.refdata.model.SettlementInstruction;
import com.example.refdata.model.Status;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;

/**
 * The SELECT statement and the row-to-record mapping of each table. Enum codes go through {@code fromDb} (null when
 * unknown, for the validator to report); NULL end dates become the model's open-ended sentinels; DATE becomes an epoch
 * day and TIMESTAMP epoch milliseconds, both read as UTC. Low-cardinality string columns (country and currency codes,
 * cities, regions, BICs) are de-duplicated through the reader's {@link StringCache}; high-cardinality ones (names,
 * references, account numbers, street lines, postal codes) are not.
 */
final class RowMappers {
    private RowMappers() {
    }

    /** Reads rows of one kind and pushes them into the sink. */
    interface TableReader {
        String sql();

        void push(ResultSet rs, Row row, RefDataSink sink) throws SQLException;
    }

    /** Per-thread state of one table read: the UTC calendar for dates and the string de-duplication cache. */
    static final class Row {
        final Calendar utc = new GregorianCalendar(TimeZone.getTimeZone("UTC"), Locale.ROOT);
        private final StringCache strings = new StringCache();

        /** Canonical instance of a low-cardinality column value. */
        String shared(ResultSet rs, int column) throws SQLException {
            return strings.dedupe(rs.getString(column));
        }
    }

    static TableReader forKind(EntityKind kind) {
        return switch (kind) {
            case COUNTRY -> COUNTRY;
            case CURRENCY -> CURRENCY;
            case CLIENT -> CLIENT;
            case ADDRESS -> ADDRESS;
            case ACCOUNT -> ACCOUNT;
            case SETTLEMENT_INSTRUCTION -> SETTLEMENT_INSTRUCTION;
        };
    }

    private static final TableReader COUNTRY = new TableReader() {
        public String sql() { return "SELECT country_code, iso3, numeric_code, name, region FROM COUNTRY ORDER BY country_code"; }

        public void push(ResultSet rs, Row row, RefDataSink sink) throws SQLException {
            sink.country(new Country(rs.getString(1), rs.getString(2), rs.getInt(3), rs.getString(4), row.shared(rs, 5)));
        }
    };

    private static final TableReader CURRENCY = new TableReader() {
        public String sql() { return "SELECT currency_code, numeric_code, name, minor_units, active FROM CURRENCY ORDER BY currency_code"; }

        public void push(ResultSet rs, Row row, RefDataSink sink) throws SQLException {
            sink.currency(new Currency(rs.getString(1), rs.getInt(2), rs.getString(3), rs.getInt(4), rs.getBoolean(5)));
        }
    };

    private static final TableReader CLIENT = new TableReader() {
        public String sql() {
            return "SELECT client_id, client_ref, lei, legal_name, short_name, client_type, status, incorporation_country, parent_client_id, "
                    + "risk_rating, onboarded_date, row_version, updated_at FROM CLIENT ORDER BY client_id";
        }

        public void push(ResultSet rs, Row row, RefDataSink sink) throws SQLException {
            long parent = rs.getLong(9);
            if (rs.wasNull()) parent = Client.NO_PARENT;
            sink.client(new Client(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getString(5),
                    ClientType.fromDb(rs.getString(6)), Status.fromDb(rs.getString(7)), row.shared(rs, 8), parent,
                    rs.getInt(10), epochDay(rs.getDate(11, row.utc)), rs.getInt(12), epochMillis(rs.getTimestamp(13, row.utc))));
        }
    };

    private static final TableReader ADDRESS = new TableReader() {
        public String sql() {
            return "SELECT address_id, client_id, address_type, line1, line2, line3, city, region, postal_code, country_code, is_primary, "
                    + "valid_from, valid_to, row_version, updated_at FROM ADDRESS ORDER BY address_id";
        }

        public void push(ResultSet rs, Row row, RefDataSink sink) throws SQLException {
            sink.address(new Address(rs.getLong(1), rs.getLong(2), AddressType.fromDb(rs.getString(3)), rs.getString(4), rs.getString(5),
                    rs.getString(6), row.shared(rs, 7), row.shared(rs, 8), rs.getString(9), row.shared(rs, 10), rs.getBoolean(11),
                    epochDay(rs.getDate(12, row.utc)), epochDayOr(rs.getDate(13, row.utc), Address.OPEN_ENDED), rs.getInt(14), epochMillis(rs.getTimestamp(15, row.utc))));
        }
    };

    private static final TableReader ACCOUNT = new TableReader() {
        public String sql() {
            return "SELECT account_id, account_number, client_id, account_type, base_currency, booking_country, status, opened_date, closed_date, "
                    + "row_version, updated_at FROM ACCOUNT ORDER BY account_id";
        }

        public void push(ResultSet rs, Row row, RefDataSink sink) throws SQLException {
            sink.account(new Account(rs.getLong(1), rs.getString(2), rs.getLong(3), AccountType.fromDb(rs.getString(4)), row.shared(rs, 5),
                    row.shared(rs, 6), Status.fromDb(rs.getString(7)), epochDay(rs.getDate(8, row.utc)), epochDayOr(rs.getDate(9, row.utc), Account.NOT_CLOSED),
                    rs.getInt(10), epochMillis(rs.getTimestamp(11, row.utc))));
        }
    };

    private static final TableReader SETTLEMENT_INSTRUCTION = new TableReader() {
        public String sql() {
            return "SELECT ssi_id, account_id, currency_code, asset_class, beneficiary_bic, agent_bic, agent_account, is_default, effective_from, "
                    + "effective_to, row_version, updated_at FROM SETTLEMENT_INSTRUCTION ORDER BY ssi_id";
        }

        public void push(ResultSet rs, Row row, RefDataSink sink) throws SQLException {
            sink.settlementInstruction(new SettlementInstruction(rs.getLong(1), rs.getLong(2), row.shared(rs, 3), AssetClass.fromDb(rs.getString(4)),
                    row.shared(rs, 5), row.shared(rs, 6), rs.getString(7), rs.getBoolean(8), epochDay(rs.getDate(9, row.utc)),
                    epochDayOr(rs.getDate(10, row.utc), SettlementInstruction.OPEN_ENDED), rs.getInt(11), epochMillis(rs.getTimestamp(12, row.utc))));
        }
    };

    /** Epoch day of a DATE read with the UTC calendar; a NULL mandatory date maps to 0 (the validator has no rule for it, the column is NOT NULL). */
    static int epochDay(Date date) {
        return date == null ? 0 : (int) Math.floorDiv(date.getTime(), 86_400_000L);
    }

    static int epochDayOr(Date date, int whenNull) {
        return date == null ? whenNull : epochDay(date);
    }

    static long epochMillis(Timestamp ts) {
        return ts == null ? 0L : ts.getTime();
    }
}
