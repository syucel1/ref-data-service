package com.example.refdata.db;

import com.example.refdata.model.EntityKind;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * The reference-data schema (design sections 5.1 and 5.2) in standard SQL accepted by Apache Derby: the six entity
 * tables plus REFDATA_RELEASE. Tables are created without constraints so a bulk load can run unindexed; the
 * primary, unique and foreign keys are separate statements added afterwards ({@link #createStatements()} is both in
 * the right order). {@code TIMESTAMP} columns hold UTC wall-clock values; {@code DATE} columns are calendar dates.
 */
public final class RefDataSchema {
    private RefDataSchema() {
    }

    /** The data-version table (design 5.2), outside the six entity tables. */
    public static final String RELEASE_TABLE = "REFDATA_RELEASE";

    /** Table names in foreign-key (= load) order: the six entity tables followed by the release table. */
    public static final List<String> TABLES_IN_FK_ORDER = List.of(
            EntityKind.COUNTRY.tableName(), EntityKind.CURRENCY.tableName(), EntityKind.CLIENT.tableName(),
            EntityKind.ADDRESS.tableName(), EntityKind.ACCOUNT.tableName(), EntityKind.SETTLEMENT_INSTRUCTION.tableName(),
            RELEASE_TABLE);

    private static final List<String> TABLE_STATEMENTS = List.of(
            """
            CREATE TABLE COUNTRY (
                country_code CHAR(2) NOT NULL,
                iso3 CHAR(3) NOT NULL,
                numeric_code SMALLINT NOT NULL,
                name VARCHAR(100) NOT NULL,
                region VARCHAR(30) NOT NULL
            )""",
            """
            CREATE TABLE CURRENCY (
                currency_code CHAR(3) NOT NULL,
                numeric_code SMALLINT NOT NULL,
                name VARCHAR(60) NOT NULL,
                minor_units SMALLINT NOT NULL,
                active BOOLEAN NOT NULL
            )""",
            """
            CREATE TABLE CLIENT (
                client_id BIGINT NOT NULL,
                client_ref VARCHAR(20) NOT NULL,
                lei CHAR(20),
                legal_name VARCHAR(200) NOT NULL,
                short_name VARCHAR(50) NOT NULL,
                client_type VARCHAR(20) NOT NULL,
                status VARCHAR(12) NOT NULL,
                incorporation_country CHAR(2) NOT NULL,
                parent_client_id BIGINT,
                risk_rating SMALLINT NOT NULL,
                onboarded_date DATE NOT NULL,
                row_version INT NOT NULL,
                updated_at TIMESTAMP NOT NULL
            )""",
            """
            CREATE TABLE ADDRESS (
                address_id BIGINT NOT NULL,
                client_id BIGINT NOT NULL,
                address_type VARCHAR(12) NOT NULL,
                line1 VARCHAR(100) NOT NULL,
                line2 VARCHAR(100),
                line3 VARCHAR(100),
                city VARCHAR(60) NOT NULL,
                region VARCHAR(60),
                postal_code VARCHAR(16) NOT NULL,
                country_code CHAR(2) NOT NULL,
                is_primary BOOLEAN NOT NULL,
                valid_from DATE NOT NULL,
                valid_to DATE,
                row_version INT NOT NULL,
                updated_at TIMESTAMP NOT NULL
            )""",
            """
            CREATE TABLE ACCOUNT (
                account_id BIGINT NOT NULL,
                account_number VARCHAR(34) NOT NULL,
                client_id BIGINT NOT NULL,
                account_type VARCHAR(12) NOT NULL,
                base_currency CHAR(3) NOT NULL,
                booking_country CHAR(2) NOT NULL,
                status VARCHAR(12) NOT NULL,
                opened_date DATE NOT NULL,
                closed_date DATE,
                row_version INT NOT NULL,
                updated_at TIMESTAMP NOT NULL
            )""",
            """
            CREATE TABLE SETTLEMENT_INSTRUCTION (
                ssi_id BIGINT NOT NULL,
                account_id BIGINT NOT NULL,
                currency_code CHAR(3) NOT NULL,
                asset_class VARCHAR(16) NOT NULL,
                beneficiary_bic CHAR(11) NOT NULL,
                agent_bic CHAR(11) NOT NULL,
                agent_account VARCHAR(34) NOT NULL,
                is_default BOOLEAN NOT NULL,
                effective_from DATE NOT NULL,
                effective_to DATE,
                row_version INT NOT NULL,
                updated_at TIMESTAMP NOT NULL
            )""",
            """
            CREATE TABLE REFDATA_RELEASE (
                release_id BIGINT NOT NULL,
                status VARCHAR(12) NOT NULL,
                published_at TIMESTAMP NOT NULL,
                description VARCHAR(200)
            )""");

    private static final List<String> CONSTRAINT_STATEMENTS = List.of(
            "ALTER TABLE COUNTRY ADD CONSTRAINT PK_COUNTRY PRIMARY KEY (country_code)",
            "ALTER TABLE COUNTRY ADD CONSTRAINT UK_COUNTRY_ISO3 UNIQUE (iso3)",
            "ALTER TABLE COUNTRY ADD CONSTRAINT UK_COUNTRY_NUMERIC UNIQUE (numeric_code)",
            "ALTER TABLE CURRENCY ADD CONSTRAINT PK_CURRENCY PRIMARY KEY (currency_code)",
            "ALTER TABLE CURRENCY ADD CONSTRAINT UK_CURRENCY_NUMERIC UNIQUE (numeric_code)",
            "ALTER TABLE CLIENT ADD CONSTRAINT PK_CLIENT PRIMARY KEY (client_id)",
            "ALTER TABLE CLIENT ADD CONSTRAINT UK_CLIENT_REF UNIQUE (client_ref)",
            "ALTER TABLE CLIENT ADD CONSTRAINT UK_CLIENT_LEI UNIQUE (lei)",
            "ALTER TABLE CLIENT ADD CONSTRAINT FK_CLIENT_COUNTRY FOREIGN KEY (incorporation_country) REFERENCES COUNTRY (country_code)",
            "ALTER TABLE CLIENT ADD CONSTRAINT FK_CLIENT_PARENT FOREIGN KEY (parent_client_id) REFERENCES CLIENT (client_id)",
            "ALTER TABLE ADDRESS ADD CONSTRAINT PK_ADDRESS PRIMARY KEY (address_id)",
            "ALTER TABLE ADDRESS ADD CONSTRAINT FK_ADDRESS_CLIENT FOREIGN KEY (client_id) REFERENCES CLIENT (client_id)",
            "ALTER TABLE ADDRESS ADD CONSTRAINT FK_ADDRESS_COUNTRY FOREIGN KEY (country_code) REFERENCES COUNTRY (country_code)",
            "ALTER TABLE ACCOUNT ADD CONSTRAINT PK_ACCOUNT PRIMARY KEY (account_id)",
            "ALTER TABLE ACCOUNT ADD CONSTRAINT UK_ACCOUNT_NUMBER UNIQUE (account_number)",
            "ALTER TABLE ACCOUNT ADD CONSTRAINT FK_ACCOUNT_CLIENT FOREIGN KEY (client_id) REFERENCES CLIENT (client_id)",
            "ALTER TABLE ACCOUNT ADD CONSTRAINT FK_ACCOUNT_CURRENCY FOREIGN KEY (base_currency) REFERENCES CURRENCY (currency_code)",
            "ALTER TABLE ACCOUNT ADD CONSTRAINT FK_ACCOUNT_COUNTRY FOREIGN KEY (booking_country) REFERENCES COUNTRY (country_code)",
            "ALTER TABLE SETTLEMENT_INSTRUCTION ADD CONSTRAINT PK_SSI PRIMARY KEY (ssi_id)",
            "ALTER TABLE SETTLEMENT_INSTRUCTION ADD CONSTRAINT UK_SSI_PERIOD UNIQUE (account_id, currency_code, asset_class, effective_from)",
            "ALTER TABLE SETTLEMENT_INSTRUCTION ADD CONSTRAINT FK_SSI_ACCOUNT FOREIGN KEY (account_id) REFERENCES ACCOUNT (account_id)",
            "ALTER TABLE SETTLEMENT_INSTRUCTION ADD CONSTRAINT FK_SSI_CURRENCY FOREIGN KEY (currency_code) REFERENCES CURRENCY (currency_code)",
            "ALTER TABLE REFDATA_RELEASE ADD CONSTRAINT PK_REFDATA_RELEASE PRIMARY KEY (release_id)");

    /** {@code CREATE TABLE} statements only (no keys), in foreign-key order. */
    public static List<String> tableStatements() {
        return TABLE_STATEMENTS;
    }

    /** Primary, unique and foreign keys, to run after {@link #tableStatements()} (NULL LEIs do not collide). */
    public static List<String> constraintStatements() {
        return CONSTRAINT_STATEMENTS;
    }

    /** Everything needed to create the complete schema, in execution order. */
    public static List<String> createStatements() {
        List<String> all = new java.util.ArrayList<>(TABLE_STATEMENTS);
        all.addAll(CONSTRAINT_STATEMENTS);
        return List.copyOf(all);
    }

    /** Creates the complete schema (tables, then constraints) on the connection's current schema. */
    public static void create(Connection connection) throws SQLException {
        execute(connection, createStatements());
    }

    /** Drops the seven tables if they exist, children first, so {@link #create} can run again. */
    public static void dropIfExists(Connection connection) throws SQLException {
        try (Statement st = connection.createStatement()) {
            for (int i = TABLES_IN_FK_ORDER.size() - 1; i >= 0; i--) {
                String table = TABLES_IN_FK_ORDER.get(i);
                if (tableExists(connection, table)) st.executeUpdate("DROP TABLE " + table);
            }
        }
        if (!connection.getAutoCommit()) connection.commit();
    }

    /** True when a table of that (upper-case) name exists in the connection's current schema. */
    public static boolean tableExists(Connection connection, String table) throws SQLException {
        try (var rs = connection.getMetaData().getTables(null, connection.getSchema(), table.toUpperCase(java.util.Locale.ROOT), null)) {
            return rs.next();
        }
    }

    /** Runs each statement in order, committing at the end when auto-commit is off. */
    public static void execute(Connection connection, List<String> statements) throws SQLException {
        try (Statement st = connection.createStatement()) {
            for (String sql : statements) st.executeUpdate(sql);
        }
        if (!connection.getAutoCommit()) connection.commit();
    }
}
