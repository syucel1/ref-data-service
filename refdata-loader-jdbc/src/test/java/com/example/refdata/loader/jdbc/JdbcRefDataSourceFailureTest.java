package com.example.refdata.loader.jdbc;

import com.example.refdata.db.BadDataInjector;
import com.example.refdata.db.DataGenerator;
import com.example.refdata.db.EmbeddedDerby;
import com.example.refdata.db.RefDataSchema;
import com.example.refdata.model.Account;
import com.example.refdata.model.Address;
import com.example.refdata.model.Client;
import com.example.refdata.model.Country;
import com.example.refdata.model.Currency;
import com.example.refdata.model.RefDataSink;
import com.example.refdata.model.SettlementInstruction;
import com.example.refdata.model.EntityKind;
import com.example.refdata.model.RefDataSource;
import com.example.refdata.model.RefDataStoreBuilder;
import com.example.refdata.model.validation.ValidationException;
import com.example.refdata.model.validation.ValidationReport;
import com.example.refdata.model.validation.Violation;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Bad data and unstable databases: what the source and the validator report. */
public class JdbcRefDataSourceFailureTest {

    @Test
    public void injectedBadDataSurfacesAsValidationViolations() throws Exception {
        try (EmbeddedDerby db = EmbeddedDerby.inMemory("loader-bad"); Connection c = db.connect()) {
            new DataGenerator(500, 11).generate(c);
            BadDataInjector bad = new BadDataInjector(c);
            long unknownEnum = bad.unknownEnumCode();
            long orphan = bad.orphanAccount();
            long duplicate = bad.duplicateAccountNumber();
            long nulled = bad.nullMandatoryColumn();
            long looped = bad.hierarchyCycle();
            long overlap = bad.overlappingDefaultSsis();

            RefDataStoreBuilder builder = new RefDataStoreBuilder();
            new JdbcRefDataSource(db.url(), JdbcLoadConfig.defaults()).load(builder);
            try {
                builder.build();
                fail("expected validation to fail");
            } catch (ValidationException e) {
                ValidationReport report = e.report();
                Map<Violation.Rule, Integer> byRule = report.countByRule();
                assertTrue(report.toString(), byRule.containsKey(Violation.Rule.MANDATORY_MISSING));
                assertTrue(report.toString(), byRule.containsKey(Violation.Rule.MISSING_REFERENCE));
                assertTrue(report.toString(), byRule.containsKey(Violation.Rule.DUPLICATE_KEY));
                assertTrue(report.toString(), byRule.containsKey(Violation.Rule.HIERARCHY_CYCLE));
                assertTrue(report.toString(), byRule.containsKey(Violation.Rule.OVERLAPPING_PERIODS));
                assertTrue(has(report, Violation.Rule.MANDATORY_MISSING, EntityKind.ACCOUNT, unknownEnum));   // unknown account_type -> null type
                assertTrue(has(report, Violation.Rule.MISSING_REFERENCE, EntityKind.ACCOUNT, orphan));
                assertTrue(has(report, Violation.Rule.DUPLICATE_KEY, EntityKind.ACCOUNT, duplicate));
                assertTrue(has(report, Violation.Rule.MANDATORY_MISSING, EntityKind.CLIENT, nulled));
                assertTrue(has(report, Violation.Rule.HIERARCHY_CYCLE, EntityKind.CLIENT, looped));
                assertTrue(has(report, Violation.Rule.OVERLAPPING_PERIODS, EntityKind.SETTLEMENT_INSTRUCTION, overlap)
                        || report.countByRule().get(Violation.Rule.OVERLAPPING_PERIODS) > 0);
            }
        }
    }

    private static boolean has(ValidationReport report, Violation.Rule rule, EntityKind kind, long id) {
        return report.violations().stream().anyMatch(v -> v.rule() == rule && v.kind() == kind && v.id() == id);
    }

    @Test
    public void releaseChangingDuringTheLoadFailsTheLoad() throws Exception {
        try (EmbeddedDerby db = EmbeddedDerby.inMemory("loader-release"); Connection c = db.connect()) {
            new DataGenerator(200, 5).generate(c);
            RefDataSink sink = new RefDataSink() {
                private boolean published;

                @Override public void client(Client client) {
                    if (!published) {
                        published = true;
                        publishRelease(c, 2);
                    }
                }
                @Override public void country(Country x) { }
                @Override public void currency(Currency x) { }
                @Override public void address(Address x) { }
                @Override public void account(Account x) { }
                @Override public void settlementInstruction(SettlementInstruction x) { }
            };
            try {
                new JdbcRefDataSource(db.url(), new JdbcLoadConfig(100, 2, true)).load(sink);
                fail("expected the load to notice the new release");
            } catch (JdbcLoadException e) {
                assertTrue(e.getMessage(), e.getMessage().contains("changed during the load"));
                assertTrue(e.getMessage(), e.getMessage().contains("release 1 before, 2 after"));
            }
        }
    }

    private static void publishRelease(Connection c, long id) {
        try (Statement st = c.createStatement()) {
            st.executeUpdate("INSERT INTO REFDATA_RELEASE (release_id, status, published_at, description) VALUES (" + id + ", 'PUBLISHED', CURRENT_TIMESTAMP, 'test')");
        } catch (java.sql.SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    public void missingReleaseTableMeansVersionZero() throws Exception {
        try (EmbeddedDerby db = EmbeddedDerby.inMemory("loader-norelease"); Connection c = db.connect()) {
            RefDataSchema.execute(c, RefDataSchema.tableStatements().subList(0, 6));
            JdbcRefDataSource source = new JdbcRefDataSource(db.url(), JdbcLoadConfig.defaults());
            RefDataSource.Preflight p = source.preflight();
            assertEquals(0L, p.dataVersion());
            assertEquals(Long.valueOf(0), p.rowCounts().get(EntityKind.CLIENT));
            assertEquals(0L, source.load(new RefDataStoreBuilder()));
        }
    }

    @Test
    public void missingTableIsReportedByName() throws Exception {
        try (EmbeddedDerby db = EmbeddedDerby.inMemory("loader-notable"); Connection c = db.connect()) {
            RefDataSchema.execute(c, RefDataSchema.tableStatements().subList(0, 5));   // no SETTLEMENT_INSTRUCTION
            try {
                new JdbcRefDataSource(db.url(), JdbcLoadConfig.defaults()).load(new RefDataStoreBuilder());
                fail("expected failure");
            } catch (JdbcLoadException e) {
                assertTrue(e.getMessage(), e.getMessage().startsWith("loading SETTLEMENT_INSTRUCTION"));
            }
        }
    }

    @Test
    public void unreachableDatabaseIsReportedWithoutCredentials() {
        try {
            new JdbcRefDataSource("jdbc:derby:/nonexistent/dir/db;user=bob;password=secret", JdbcLoadConfig.defaults()).preflight();
            fail("expected failure");
        } catch (JdbcLoadException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("pre-flight of jdbc:derby:/nonexistent/dir/db;user=***;password=*** failed"));
            assertTrue(e.getMessage(), !e.getMessage().contains("secret"));
        }
    }
}
