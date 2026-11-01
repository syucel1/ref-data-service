package com.example.refdata.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Corrupts a generated database in controlled ways so the validator rules (design 13.1, S-5 to S-8 and S-14) can
 * be exercised end to end. Where a database constraint would prevent the corruption, that constraint is dropped
 * first (this is what the real world looks like when the constraint was never there). Every method commits and
 * returns the id of the row it damaged. Intended for tests and fault-injection runs only.
 */
public final class BadDataInjector {
    private static final Logger log = LoggerFactory.getLogger(BadDataInjector.class);

    private final Connection connection;

    public BadDataInjector(Connection connection) {
        this.connection = connection;
    }

    /** S-7: points the account with the highest id at a client that does not exist. */
    public long orphanAccount() throws SQLException {
        dropConstraint("ACCOUNT", "FK_ACCOUNT_CLIENT");
        long accountId = maxId("ACCOUNT", "account_id");
        long missingClient = maxId("CLIENT", "client_id") + 1_000_000;
        update("UPDATE ACCOUNT SET client_id = " + missingClient + " WHERE account_id = " + accountId);
        log.info("injected orphan: account {} -> client {}", accountId, missingClient);
        return accountId;
    }

    /** S-6: gives the account with the highest id the account number of account 1. */
    public long duplicateAccountNumber() throws SQLException {
        dropConstraint("ACCOUNT", "UK_ACCOUNT_NUMBER");
        long accountId = maxId("ACCOUNT", "account_id");
        update("UPDATE ACCOUNT SET account_number = (SELECT account_number FROM ACCOUNT WHERE account_id = 1) WHERE account_id = " + accountId);
        log.info("injected duplicate account_number on account {}", accountId);
        return accountId;
    }

    /** S-5: sets {@code legal_name} to NULL on the client with the highest id (the column is made nullable first). */
    public long nullMandatoryColumn() throws SQLException {
        long clientId = maxId("CLIENT", "client_id");
        update("ALTER TABLE CLIENT ALTER COLUMN legal_name NULL");
        update("UPDATE CLIENT SET legal_name = NULL WHERE client_id = " + clientId);
        log.info("injected NULL legal_name on client {}", clientId);
        return clientId;
    }

    /** S-5: gives the account with the highest id an account_type the model does not know. */
    public long unknownEnumCode() throws SQLException {
        long accountId = maxId("ACCOUNT", "account_id");
        update("UPDATE ACCOUNT SET account_type = 'CRYPTO' WHERE account_id = " + accountId);
        log.info("injected unknown account_type on account {}", accountId);
        return accountId;
    }

    /**
     * S-8: makes a top-level client the child of one of its own children (or of itself when there is no
     * hierarchy at all), so the parent chain loops. Returns the client whose parent was changed.
     */
    public long hierarchyCycle() throws SQLException {
        long child = queryLong("SELECT MIN(c.client_id) FROM CLIENT c JOIN CLIENT p ON p.client_id = c.parent_client_id WHERE p.parent_client_id IS NULL");
        long parent = child == 0 ? maxId("CLIENT", "client_id") : queryLong("SELECT parent_client_id FROM CLIENT WHERE client_id = " + child);
        if (child == 0) child = parent;
        update("UPDATE CLIENT SET parent_client_id = " + child + " WHERE client_id = " + parent);
        log.info("injected hierarchy cycle: client {} now has parent {}", parent, child);
        return parent;
    }

    /**
     * S-14: makes the earlier of two consecutive default instructions of the same (account, currency, asset class)
     * open-ended so that it overlaps the next one. Returns the id of the extended instruction.
     */
    public long overlappingDefaultSsis() throws SQLException {
        String sql = "SELECT a.ssi_id, b.ssi_id FROM SETTLEMENT_INSTRUCTION a JOIN SETTLEMENT_INSTRUCTION b "
                + "ON a.account_id = b.account_id AND a.currency_code = b.currency_code AND a.asset_class = b.asset_class "
                + "AND a.effective_to = b.effective_from WHERE a.is_default AND b.is_default ORDER BY a.ssi_id FETCH FIRST 1 ROWS ONLY";
        long earlier, later;
        try (Statement st = connection.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            if (!rs.next()) throw new IllegalStateException("no pair of consecutive default instructions to overlap");
            earlier = rs.getLong(1);
            later = rs.getLong(2);
        }
        update("UPDATE SETTLEMENT_INSTRUCTION SET effective_to = NULL WHERE ssi_id = " + earlier);
        log.info("injected overlap: default SSI {} now open-ended over SSI {}", earlier, later);
        return earlier;
    }

    private void dropConstraint(String table, String name) throws SQLException {
        update("ALTER TABLE " + table + " DROP CONSTRAINT " + name);
    }

    private long maxId(String table, String column) throws SQLException {
        return queryLong("SELECT MAX(" + column + ") FROM " + table);
    }

    private long queryLong(String sql) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getLong(1) : 0L;
        }
    }

    private void update(String sql) throws SQLException {
        try (Statement st = connection.createStatement()) {
            st.executeUpdate(sql);
        }
        if (!connection.getAutoCommit()) connection.commit();
    }
}
