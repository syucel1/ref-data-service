package com.example.refdata.loader.jdbc;

import com.example.refdata.db.DataGenerator;
import com.example.refdata.db.EmbeddedDerby;
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
import com.example.refdata.model.RefDataSource;
import com.example.refdata.model.RefDataStore;
import com.example.refdata.model.RefDataStoreBuilder;
import com.example.refdata.model.SettlementInstruction;
import com.example.refdata.model.Status;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/** Loads a generated TINY database through the JDBC source and compares the result with SQL. */
public class JdbcRefDataSourceTest {
    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSSSSSSSS][.SSSSSS][.SSS][.S]");

    private static EmbeddedDerby db;
    private static Connection sql;
    private static RefDataStore store;
    private static long dataVersion;

    @BeforeClass
    public static void generateAndLoad() throws Exception {
        db = EmbeddedDerby.inMemory("loader-test");
        sql = db.connect();
        new DataGenerator(DataGenerator.Profile.TINY, 3).generate(sql);
        RefDataStoreBuilder builder = new RefDataStoreBuilder();
        dataVersion = new JdbcRefDataSource(db.url(), JdbcLoadConfig.defaults()).load(builder);
        RefDataStoreBuilder.Result result = builder.dataVersion(dataVersion).build();
        assertTrue(result.report().toString(), result.report().isClean());
        store = result.store();
    }

    @AfterClass
    public static void closeDb() throws Exception {
        sql.close();
        db.close();
    }

    @Test
    public void preflightReportsCountsAndReleaseId() throws Exception {
        RefDataSource.Preflight p = new JdbcRefDataSource(db.url(), JdbcLoadConfig.defaults()).preflight();
        for (EntityKind k : EntityKind.values()) assertEquals(k.toString(), count(k.tableName()), (long) p.rowCounts().get(k));
        assertEquals(DataGenerator.RELEASE_ID, p.dataVersion());
        assertEquals(db.url(), p.sourceDescription());
    }

    @Test
    public void loadReturnsTheReleaseIdAndEveryRow() throws Exception {
        assertEquals(DataGenerator.RELEASE_ID, dataVersion);
        assertEquals(dataVersion, store.dataVersion());
        for (EntityKind k : EntityKind.values()) assertEquals(k.toString(), count(k.tableName()), store.count(k));
    }

    @Test
    public void countryAndCurrencyRowsMatchSql() throws Exception {
        try (ResultSet rs = query("SELECT * FROM COUNTRY WHERE country_code = 'CH'")) {
            Country c = store.country("CH");
            assertEquals(rs.getString("iso3"), c.iso3());
            assertEquals(rs.getInt("numeric_code"), c.numericCode());
            assertEquals(rs.getString("name"), c.name());
            assertEquals(rs.getString("region"), c.region());
        }
        try (ResultSet rs = query("SELECT * FROM CURRENCY WHERE currency_code = 'JPY'")) {
            Currency c = store.currency("JPY");
            assertEquals(rs.getInt("numeric_code"), c.numericCode());
            assertEquals(rs.getString("name"), c.name());
            assertEquals(rs.getInt("minor_units"), c.minorUnits());
            assertEquals(rs.getBoolean("active"), c.active());
        }
    }

    @Test
    public void clientRowsMatchSqlFieldByField() throws Exception {
        long withParentAndLei = scalar("SELECT MIN(client_id) FROM CLIENT WHERE parent_client_id IS NOT NULL AND lei IS NOT NULL");
        long topLevelWithoutLei = scalar("SELECT MIN(client_id) FROM CLIENT WHERE parent_client_id IS NULL AND lei IS NULL");
        for (long id : new long[]{1, withParentAndLei, topLevelWithoutLei, store.count(EntityKind.CLIENT)}) {
            try (ResultSet rs = query("SELECT * FROM CLIENT WHERE client_id = " + id)) {
                Client c = store.client(id);
                assertNotNull(c);
                assertEquals(rs.getString("client_ref"), c.ref());
                assertEquals(rs.getString("lei"), c.lei());
                assertEquals(rs.getString("legal_name"), c.legalName());
                assertEquals(rs.getString("short_name"), c.shortName());
                assertEquals(ClientType.valueOf(rs.getString("client_type")), c.type());
                assertEquals(Status.valueOf(rs.getString("status")), c.status());
                assertEquals(rs.getString("incorporation_country"), c.incorporationCountry());
                long parent = rs.getLong("parent_client_id");
                assertEquals(rs.wasNull() ? Client.NO_PARENT : parent, c.parentClientId());
                assertEquals(rs.getInt("risk_rating"), c.riskRating());
                assertEquals(epochDay(rs.getString("onboarded_date")), c.onboardedDate());
                assertEquals(rs.getInt("row_version"), c.rowVersion());
                assertEquals(epochMillis(rs.getString("updated_at")), c.updatedAt());
            }
        }
        assertTrue(store.client(withParentAndLei).hasParent());
        assertNull(store.client(topLevelWithoutLei).lei());
        assertEquals(store.client(1), store.clientByRef(store.client(1).ref()));
    }

    @Test
    public void addressRowsMatchSqlIncludingNullsAndOpenEnd() throws Exception {
        long openEnded = scalar("SELECT MIN(address_id) FROM ADDRESS WHERE valid_to IS NULL AND line2 IS NULL");
        long dated = scalar("SELECT MIN(address_id) FROM ADDRESS WHERE valid_to IS NOT NULL AND line2 IS NOT NULL");
        for (long id : new long[]{openEnded, dated}) {
            try (ResultSet rs = query("SELECT * FROM ADDRESS WHERE address_id = " + id)) {
                Address a = store.address(id);
                assertEquals(rs.getLong("client_id"), a.clientId());
                assertEquals(AddressType.valueOf(rs.getString("address_type")), a.type());
                assertEquals(rs.getString("line1"), a.line1());
                assertEquals(rs.getString("line2"), a.line2());
                assertEquals(rs.getString("line3"), a.line3());
                assertEquals(rs.getString("city"), a.city());
                assertEquals(rs.getString("region"), a.region());
                assertEquals(rs.getString("postal_code"), a.postalCode());
                assertEquals(rs.getString("country_code"), a.countryCode());
                assertEquals(rs.getBoolean("is_primary"), a.primary());
                assertEquals(epochDay(rs.getString("valid_from")), a.validFrom());
                String to = rs.getString("valid_to");
                assertEquals(to == null ? Address.OPEN_ENDED : epochDay(to), a.validTo());
                assertEquals(rs.getInt("row_version"), a.rowVersion());
                assertEquals(epochMillis(rs.getString("updated_at")), a.updatedAt());
            }
        }
        assertNull(store.address(openEnded).line2());
        assertEquals(Address.OPEN_ENDED, store.address(openEnded).validTo());
        assertTrue(store.address(dated).validTo() < Address.OPEN_ENDED);
    }

    @Test
    public void accountRowsMatchSqlIncludingClosedDate() throws Exception {
        long closed = scalar("SELECT MIN(account_id) FROM ACCOUNT WHERE closed_date IS NOT NULL");
        long open = scalar("SELECT MIN(account_id) FROM ACCOUNT WHERE closed_date IS NULL");
        for (long id : new long[]{closed, open}) {
            try (ResultSet rs = query("SELECT * FROM ACCOUNT WHERE account_id = " + id)) {
                Account a = store.account(id);
                assertEquals(rs.getString("account_number"), a.number());
                assertEquals(rs.getLong("client_id"), a.clientId());
                assertEquals(AccountType.valueOf(rs.getString("account_type")), a.type());
                assertEquals(rs.getString("base_currency"), a.baseCurrency());
                assertEquals(rs.getString("booking_country"), a.bookingCountry());
                assertEquals(Status.valueOf(rs.getString("status")), a.status());
                assertEquals(epochDay(rs.getString("opened_date")), a.openedDate());
                String closedDate = rs.getString("closed_date");
                assertEquals(closedDate == null ? Account.NOT_CLOSED : epochDay(closedDate), a.closedDate());
                assertEquals(rs.getInt("row_version"), a.rowVersion());
                assertEquals(epochMillis(rs.getString("updated_at")), a.updatedAt());
            }
        }
        assertTrue(store.account(closed).isClosed());
        assertFalse(store.account(open).isClosed());
        assertEquals(store.account(open), store.accountByNumber(store.account(open).number()));
    }

    @Test
    public void settlementInstructionRowsMatchSql() throws Exception {
        long dflt = scalar("SELECT MIN(ssi_id) FROM SETTLEMENT_INSTRUCTION WHERE is_default AND effective_to IS NOT NULL");
        long other = scalar("SELECT MIN(ssi_id) FROM SETTLEMENT_INSTRUCTION WHERE NOT is_default AND effective_to IS NULL");
        for (long id : new long[]{dflt, other}) {
            try (ResultSet rs = query("SELECT * FROM SETTLEMENT_INSTRUCTION WHERE ssi_id = " + id)) {
                SettlementInstruction s = store.settlementInstruction(id);
                assertEquals(rs.getLong("account_id"), s.accountId());
                assertEquals(rs.getString("currency_code"), s.currencyCode());
                assertEquals(AssetClass.valueOf(rs.getString("asset_class")), s.assetClass());
                assertEquals(rs.getString("beneficiary_bic"), s.beneficiaryBic());
                assertEquals(rs.getString("agent_bic"), s.agentBic());
                assertEquals(rs.getString("agent_account"), s.agentAccount());
                assertEquals(rs.getBoolean("is_default"), s.isDefault());
                assertEquals(epochDay(rs.getString("effective_from")), s.effectiveFrom());
                String to = rs.getString("effective_to");
                assertEquals(to == null ? SettlementInstruction.OPEN_ENDED : epochDay(to), s.effectiveTo());
                assertEquals(rs.getInt("row_version"), s.rowVersion());
                assertEquals(epochMillis(rs.getString("updated_at")), s.updatedAt());
            }
        }
        SettlementInstruction d = store.settlementInstruction(dflt);
        assertEquals(d, store.defaultSsi(d.accountId(), d.currencyCode(), d.assetClass(), d.effectiveFrom()));
    }

    @Test
    public void repeatedLowCardinalityStringsShareOneInstance() throws Exception {
        Address first = null;
        for (Address a : store.addresses()) {
            if (first == null) { first = a; continue; }
            if (a.city().equals(first.city())) {
                assertSame("city", first.city(), a.city());
                assertSame("country_code", store.address(1).countryCode(), store.address(1).countryCode());
                break;
            }
        }
        Account a1 = store.account(1);
        for (Account a : store.accounts()) {
            if (a.id() != a1.id() && a.baseCurrency().equals(a1.baseCurrency())) { assertSame(a1.baseCurrency(), a.baseCurrency()); break; }
        }
        Client c1 = store.client(1);
        for (Client c : store.clients()) {
            if (c.id() != c1.id() && c.incorporationCountry().equals(c1.incorporationCountry())) { assertSame(c1.incorporationCountry(), c.incorporationCountry()); break; }
        }
    }

    @Test
    public void stripsCredentialsFromUrls() {
        assertEquals("jdbc:postgresql://***@host/db", JdbcRefDataSource.stripCredentials("jdbc:postgresql://bob:s3cret@host/db"));
        assertEquals("jdbc:derby:x;user=***;password=***", JdbcRefDataSource.stripCredentials("jdbc:derby:x;user=bob;password=s3cret"));
        assertEquals("jdbc:derby:memory:plain", JdbcRefDataSource.stripCredentials("jdbc:derby:memory:plain"));
    }

    // ---- helpers: independent conversions through the textual SQL representation

    private static int epochDay(String date) {
        return (int) LocalDate.parse(date).toEpochDay();
    }

    private static long epochMillis(String timestamp) {
        return LocalDateTime.parse(timestamp, TS).toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    private static ResultSet query(String q) throws Exception {
        Statement st = sql.createStatement();
        ResultSet rs = st.executeQuery(q);
        assertTrue("no row for " + q, rs.next());
        return rs;
    }

    private static long count(String table) throws Exception {
        return scalar("SELECT COUNT(*) FROM " + table);
    }

    private static long scalar(String q) throws Exception {
        try (ResultSet rs = query(q)) {
            return rs.getLong(1);
        }
    }
}
