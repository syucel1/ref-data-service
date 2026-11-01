package com.example.refdata.db;

import com.example.refdata.model.EntityKind;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DataGeneratorTest {
    private static final long SEED = 42;
    private static EmbeddedDerby db;
    private static Connection c;
    private static DataGenerator.Result result;

    @BeforeClass
    public static void generateTiny() throws Exception {
        db = EmbeddedDerby.inMemory("gen-test");
        c = db.connect();
        result = new DataGenerator(DataGenerator.Profile.TINY, SEED).generate(c);
    }

    @AfterClass
    public static void closeDb() throws Exception {
        c.close();
        db.close();
    }

    @Test
    public void countsMatchTheProfileWithinTolerance() throws Exception {
        Map<EntityKind, Long> expected = DataGenerator.expectedRows(DataGenerator.Profile.TINY.clientCount());
        for (EntityKind k : EntityKind.values()) {
            long actual = TableDump.count(c, k.tableName());
            assertEquals(k + " reported count", actual, (long) result.rowCounts().get(k));
            long want = expected.get(k);
            if (k == EntityKind.CLIENT || k == EntityKind.COUNTRY || k == EntityKind.CURRENCY) assertEquals(k.toString(), want, actual);
            else assertTrue(k + ": expected about " + want + " but was " + actual, Math.abs(actual - want) <= want * 0.25);
        }
        assertEquals(1, TableDump.count(c, RefDataSchema.RELEASE_TABLE));
        assertEquals(1, TableDump.scalar(c, "SELECT release_id FROM REFDATA_RELEASE WHERE status = 'PUBLISHED'"));
    }

    @Test
    public void sameSeedGivesIdenticalRowsAndDifferentSeedDoesNot() throws Exception {
        try (EmbeddedDerby same = EmbeddedDerby.inMemory("gen-same"); Connection c2 = same.connect();
             EmbeddedDerby other = EmbeddedDerby.inMemory("gen-other"); Connection c3 = other.connect()) {
            new DataGenerator(DataGenerator.Profile.TINY, SEED).generate(c2);
            new DataGenerator(DataGenerator.Profile.TINY, SEED + 1).generate(c3);
            for (int i = 0; i < RefDataSchema.TABLES_IN_FK_ORDER.size(); i++) {
                String table = RefDataSchema.TABLES_IN_FK_ORDER.get(i);
                List<String> a = TableDump.rows(c, table, TableDump.ORDER_BY[i]);
                assertEquals(table, a, TableDump.rows(c2, table, TableDump.ORDER_BY[i]));
                if (table.equals("CLIENT")) assertNotEquals(a, TableDump.rows(c3, table, TableDump.ORDER_BY[i]));
            }
        }
    }

    @Test
    public void idsAreDenseFromOneAndVersionsAndTimestampsAreInRange() throws Exception {
        for (String[] t : new String[][]{{"CLIENT", "client_id"}, {"ADDRESS", "address_id"}, {"ACCOUNT", "account_id"}, {"SETTLEMENT_INSTRUCTION", "ssi_id"}}) {
            assertEquals(t[0], 1, TableDump.scalar(c, "SELECT MIN(" + t[1] + ") FROM " + t[0]));
            assertEquals(t[0], TableDump.count(c, t[0]), TableDump.scalar(c, "SELECT MAX(" + t[1] + ") FROM " + t[0]));
            assertEquals(t[0], 0, TableDump.scalar(c, "SELECT COUNT(*) FROM " + t[0] + " WHERE row_version < 1"));
            assertEquals(t[0], 0, TableDump.scalar(c, "SELECT COUNT(*) FROM " + t[0]
                    + " WHERE updated_at > TIMESTAMP('2026-10-01 00:00:00') OR updated_at < TIMESTAMP('2023-10-01 00:00:00')"));
        }
    }

    @Test
    public void shapeFollowsTheDesign() throws Exception {
        long clients = TableDump.count(c, "CLIENT");
        long withLei = TableDump.scalar(c, "SELECT COUNT(*) FROM CLIENT WHERE lei IS NOT NULL");
        assertTrue("70% LEI, was " + withLei, withLei > clients * 0.62 && withLei < clients * 0.78);
        assertEquals(0, TableDump.scalar(c, "SELECT COUNT(*) FROM CLIENT WHERE lei IS NOT NULL AND LENGTH(lei) <> 20"));

        long accounts = TableDump.count(c, "ACCOUNT");
        long closed = TableDump.scalar(c, "SELECT COUNT(*) FROM ACCOUNT WHERE status = 'CLOSED'");
        assertTrue("5% closed, was " + closed, closed > accounts * 0.03 && closed < accounts * 0.08);
        assertEquals(closed, TableDump.scalar(c, "SELECT COUNT(*) FROM ACCOUNT WHERE closed_date IS NOT NULL AND closed_date >= opened_date"));

        long inHierarchy = TableDump.scalar(c, "SELECT COUNT(*) FROM CLIENT c WHERE parent_client_id IS NOT NULL "
                + "OR EXISTS (SELECT 1 FROM CLIENT k WHERE k.parent_client_id = c.client_id)");
        assertTrue("~10% in hierarchies, was " + inHierarchy, inHierarchy > clients * 0.06 && inHierarchy < clients * 0.16);
        assertEquals("parents precede children", 0, TableDump.scalar(c, "SELECT COUNT(*) FROM CLIENT WHERE parent_client_id >= client_id"));
        assertEquals("at most four levels", 0, TableDump.scalar(c, "SELECT COUNT(*) FROM CLIENT a JOIN CLIENT b ON a.parent_client_id = b.client_id "
                + "JOIN CLIENT x ON b.parent_client_id = x.client_id JOIN CLIENT y ON x.parent_client_id = y.client_id WHERE y.parent_client_id IS NOT NULL"));

        assertEquals("every client has a registered primary address", clients, TableDump.scalar(c,
                "SELECT COUNT(DISTINCT client_id) FROM ADDRESS WHERE address_type = 'REGISTERED' AND is_primary"));
        long maxAccounts = TableDump.scalar(c, "SELECT MAX(n) FROM (SELECT COUNT(*) AS n FROM ACCOUNT GROUP BY client_id) t");
        assertTrue("skewed: some client has many accounts, max was " + maxAccounts, maxAccounts >= 9);

        assertEquals("no overlapping default SSIs", 0, TableDump.scalar(c, "SELECT COUNT(*) FROM SETTLEMENT_INSTRUCTION a "
                + "JOIN SETTLEMENT_INSTRUCTION b ON a.account_id = b.account_id AND a.currency_code = b.currency_code AND a.asset_class = b.asset_class "
                + "AND a.ssi_id <> b.ssi_id WHERE a.is_default AND b.is_default AND a.effective_from < b.effective_from "
                + "AND (a.effective_to IS NULL OR a.effective_to > b.effective_from)"));
        assertEquals("no overlapping primary addresses", 0, TableDump.scalar(c, "SELECT COUNT(*) FROM ADDRESS a "
                + "JOIN ADDRESS b ON a.client_id = b.client_id AND a.address_type = b.address_type AND a.address_id <> b.address_id "
                + "WHERE a.is_primary AND b.is_primary AND a.valid_from < b.valid_from AND (a.valid_to IS NULL OR a.valid_to > b.valid_from)"));
    }

    @Test
    public void constraintsRejectOrphansAndDuplicates() throws Exception {
        expectIntegrityViolation("INSERT INTO ACCOUNT (account_id, account_number, client_id, account_type, base_currency, booking_country, status, opened_date, row_version, updated_at) "
                + "VALUES (999999, 'X1', 99999999, 'CASH', 'USD', 'GB', 'ACTIVE', DATE('2020-01-01'), 1, CURRENT_TIMESTAMP)");
        expectIntegrityViolation("INSERT INTO ACCOUNT (account_id, account_number, client_id, account_type, base_currency, booking_country, status, opened_date, row_version, updated_at) "
                + "SELECT 999999, account_number, 1, 'CASH', 'USD', 'GB', 'ACTIVE', DATE('2020-01-01'), 1, CURRENT_TIMESTAMP FROM ACCOUNT WHERE account_id = 1");
        expectIntegrityViolation("INSERT INTO CLIENT (client_id, client_ref, lei, legal_name, short_name, client_type, status, incorporation_country, risk_rating, onboarded_date, row_version, updated_at) "
                + "SELECT 999999, 'DUP', lei, 'x', 'x', 'BANK', 'ACTIVE', 'GB', 1, DATE('2020-01-01'), 1, CURRENT_TIMESTAMP FROM CLIENT WHERE lei IS NOT NULL FETCH FIRST 1 ROWS ONLY");
        expectIntegrityViolation("INSERT INTO SETTLEMENT_INSTRUCTION SELECT ssi_id + 1000000, account_id, currency_code, asset_class, beneficiary_bic, agent_bic, agent_account, "
                + "is_default, effective_from, effective_to, row_version, updated_at FROM SETTLEMENT_INSTRUCTION WHERE ssi_id = 1");
        expectIntegrityViolation("UPDATE COUNTRY SET country_code = 'ZZ' WHERE country_code = 'GB'");   // referenced by clients
    }

    private static void expectIntegrityViolation(String sql) throws Exception {
        try (Statement st = c.createStatement()) {
            st.executeUpdate(sql);
            fail("expected a constraint violation for: " + sql);
        } catch (SQLIntegrityConstraintViolationException expected) {
            assertTrue(expected.getSQLState().startsWith("23"));
        }
    }
}
