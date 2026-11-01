package com.example.refdata.it;

import com.example.refdata.client.LoadPhase;
import com.example.refdata.client.LoadedSnapshot;
import com.example.refdata.db.DataGenerator;
import com.example.refdata.loader.jdbc.JdbcLoadConfig;
import com.example.refdata.loader.jdbc.JdbcRefDataSource;
import com.example.refdata.model.EntityKind;
import com.example.refdata.model.RefDataStore;
import com.example.refdata.server.RefDataServer;
import com.example.refdata.testing.IntegrationTest;
import org.junit.Assume;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.SplittableRandom;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Performance smoke on the on-disk MEDIUM database (skipped when it is absent): timings and lookup latency. */
@Category(IntegrationTest.class)
public class MediumSmokeIT {
    private static final Logger log = LoggerFactory.getLogger(MediumSmokeIT.class);
    static final Path MEDIUM = Path.of("/home/model/refdata-service/data/medium");
    private static final int LOOKUPS = 1_000_000;

    @Test public void mediumLoadsAndLooksUpFast() throws Exception {
        Assume.assumeTrue("no medium database at " + MEDIUM, Files.isDirectory(MEDIUM.resolve("seg0")));
        JdbcRefDataSource source = new JdbcRefDataSource("jdbc:derby:" + MEDIUM, JdbcLoadConfig.defaults());
        long t0 = System.nanoTime();
        RefDataServer server;
        try {
            server = RefDataServer.start(ItSupport.config("rds.codecs", "LZ4"), source);
        } catch (RuntimeException e) {
            Assume.assumeTrue("medium database not usable: " + e.getMessage(), false);
            return;
        }
        try (server) {
            long serverMs = (System.nanoTime() - t0) / 1_000_000;
            LoadedSnapshot loaded = ItSupport.client(ItSupport.endpoint(server)).startupTimeout(Duration.ofMinutes(5))
                    .acceptedCodecs(com.example.refdata.protocol.v1.Codec.CODEC_LZ4).build().load();
            RefDataStore store = loaded.store();
            Map<EntityKind, Long> expected = DataGenerator.expectedRows(DataGenerator.Profile.MEDIUM.clientCount());
            for (EntityKind k : EntityKind.values()) {
                assertEquals(k.name(), server.snapshot().manifest(k).getRecordCount(), store.count(k));
                log.info("{}: {} rows (generator expects {})", k, store.count(k), expected.get(k));
            }
            assertTrue(store.count(EntityKind.CLIENT) >= 100_000);
            Map<LoadPhase, Duration> phases = loaded.stats().durationByPhase();
            log.info("MEDIUM server start (db load + validate + index + encode): {} ms", serverMs);
            log.info("MEDIUM client load: total {} ms; transfer {} ms; index {} ms; {} bytes in {} chunks",
                    loaded.stats().totalDuration().toMillis(), phases.get(LoadPhase.STREAMING).toMillis(), phases.get(LoadPhase.INDEXING).toMillis(),
                    loaded.stats().bytesReceived(), loaded.stats().chunksAccepted());
            double ns = lookupLatencyNanos(store);
            log.info("MEDIUM {} random lookups (client/account by id): {} ns average", LOOKUPS, String.format("%.1f", ns));
        }
    }

    private static double lookupLatencyNanos(RefDataStore store) {
        int clients = store.clients().size(), accounts = store.accounts().size();
        long[] clientIds = new long[clients];
        for (int i = 0; i < clients; i++) clientIds[i] = store.clients().get(i).id();
        long[] accountIds = new long[accounts];
        for (int i = 0; i < accounts; i++) accountIds[i] = store.accounts().get(i).id();
        SplittableRandom rnd = new SplittableRandom(7);
        double result = 0;
        for (int round = 0; round < 3; round++) {      // first rounds warm up, the last one counts
            int hits = 0;
            long start = System.nanoTime();
            for (int i = 0; i < LOOKUPS; i++) {
                if ((i & 1) == 0) { if (store.client(clientIds[rnd.nextInt(clients)]) != null) hits++; }
                else if (store.account(accountIds[rnd.nextInt(accounts)]) != null) hits++;
            }
            result = (double) (System.nanoTime() - start) / LOOKUPS;
            assertEquals(LOOKUPS, hits);
        }
        return result;
    }
}
