package com.example.refdata.it;

import com.example.refdata.client.RefDataClient;
import com.example.refdata.db.DataGenerator;
import com.example.refdata.db.EmbeddedDerby;
import com.example.refdata.loader.jdbc.JdbcLoadConfig;
import com.example.refdata.loader.jdbc.JdbcRefDataSource;
import com.example.refdata.model.EntityKind;
import com.example.refdata.model.RefDataStore;
import com.example.refdata.model.RefDataStoreBuilder;
import com.example.refdata.model.validation.ValidationPolicy;
import com.example.refdata.server.RefDataServer;
import com.example.refdata.server.ServerConfig;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

/** Shared plumbing of the end-to-end tests: databases, servers, clients, store comparison. */
final class ItSupport {
    private static final AtomicInteger DB_NAMES = new AtomicInteger();
    static final long SEED = 42L;

    private ItSupport() {}

    /** A fresh in-memory Derby database holding the TINY profile (1,000 clients). */
    static EmbeddedDerby tinyDb() throws Exception {
        EmbeddedDerby db = EmbeddedDerby.inMemory("it-" + DB_NAMES.incrementAndGet());
        try (Connection c = db.connect()) {
            new DataGenerator(DataGenerator.Profile.TINY, SEED).generate(c);
        }
        return db;
    }

    /** Ephemeral ports on loopback, no database URL (the source is passed in), plus the given {@code rds.*} overrides. */
    static ServerConfig config(String... keyValues) {
        Properties p = new Properties();
        p.setProperty("rds.tcp.port", "0");
        p.setProperty("rds.http.port", "0");
        p.setProperty("rds.tcp.bind.address", "127.0.0.1");
        p.setProperty("rds.load.retries", "0");
        for (int i = 0; i < keyValues.length; i += 2) p.setProperty(keyValues[i], keyValues[i + 1]);
        return ServerConfig.fromProperties(p);
    }

    static JdbcRefDataSource source(EmbeddedDerby db) {
        return new JdbcRefDataSource(db.url(), JdbcLoadConfig.defaults());
    }

    static RefDataServer startServer(EmbeddedDerby db, String... keyValues) throws InterruptedException {
        return RefDataServer.start(config(keyValues), source(db));
    }

    static String endpoint(RefDataServer server) { return "127.0.0.1:" + server.tcpPort(); }

    /** A client with short timeouts and fast back-off, suitable for tests. */
    static RefDataClient.Builder client(String... endpoints) {
        return RefDataClient.builder().clientName("it-client").endpoints(endpoints)
                .connectTimeout(Duration.ofSeconds(2)).readTimeout(Duration.ofSeconds(10))
                .backoff(Duration.ofMillis(20), Duration.ofMillis(200)).startupTimeout(Duration.ofSeconds(30));
    }

    /** The store the database holds, built directly through the JDBC loader (the server's own path). */
    static RefDataStore referenceStore(EmbeddedDerby db) throws Exception {
        RefDataStoreBuilder b = new RefDataStoreBuilder().policy(ValidationPolicy.FAIL);
        long version = source(db).load(b);
        return b.dataVersion(version).build().store();
    }

    static Map<EntityKind, Long> sqlCounts(EmbeddedDerby db) throws Exception {
        Map<EntityKind, Long> counts = new EnumMap<>(EntityKind.class);
        try (Connection c = db.connect(); Statement s = c.createStatement()) {
            for (EntityKind k : EntityKind.values()) {
                try (ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM " + k.tableName())) {
                    rs.next();
                    counts.put(k, rs.getLong(1));
                }
            }
        }
        return counts;
    }

    /** Row-by-row equality of every kind (records compare by value, lists are in primary-key order). */
    static void assertSameStore(RefDataStore expected, RefDataStore actual) {
        assertEquals(expected.dataVersion(), actual.dataVersion());
        assertEquals("countries", expected.countries(), actual.countries());
        assertEquals("currencies", expected.currencies(), actual.currencies());
        assertEquals("clients", expected.clients(), actual.clients());
        assertEquals("addresses", expected.addresses(), actual.addresses());
        assertEquals("accounts", expected.accounts(), actual.accounts());
        assertEquals("settlement instructions", expected.settlementInstructions(), actual.settlementInstructions());
    }
}
