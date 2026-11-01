package com.example.refdata.it;

import com.example.refdata.client.LoadedSnapshot;
import com.example.refdata.client.RefDataClient;
import com.example.refdata.db.BadDataInjector;
import com.example.refdata.db.EmbeddedDerby;
import com.example.refdata.model.EntityKind;
import com.example.refdata.model.RefDataStore;
import com.example.refdata.protocol.v1.Codec;
import com.example.refdata.server.RefDataServer;
import com.example.refdata.server.StoreLoader;
import com.example.refdata.testing.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** Database -> server -> client on the TINY profile: full and subset loads, codecs, not-modified, validation policy. */
@Category(IntegrationTest.class)
public class EndToEndIT {
    private static final Logger log = LoggerFactory.getLogger(EndToEndIT.class);

    private EmbeddedDerby db;
    private RefDataStore reference;

    @Before public void database() throws Exception {
        db = ItSupport.tinyDb();
        reference = ItSupport.referenceStore(db);
    }

    @After public void dropDatabase() { db.close(); }

    @Test public void fullLoadMatchesTheDatabaseRowByRow() throws Exception {
        try (RefDataServer server = ItSupport.startServer(db)) {
            LoadedSnapshot loaded = ItSupport.client(ItSupport.endpoint(server)).build().load();
            ItSupport.assertSameStore(reference, loaded.store());
            Map<EntityKind, Long> sql = ItSupport.sqlCounts(db);
            for (EntityKind k : EntityKind.values()) assertEquals(k.name(), (long) sql.get(k), loaded.store().count(k));
            assertEquals(1000, loaded.store().count(EntityKind.CLIENT));
            assertArrayEquals(server.snapshot().snapshotId(), loaded.info().getSnapshotId().toByteArray());
            assertArrayEquals(server.snapshot().contentSha256(), loaded.info().getContentSha256().toByteArray());
            assertEquals(server.snapshot().info(), loaded.info());
            for (EntityKind k : EntityKind.values()) {
                assertArrayEquals(k.name(), server.snapshot().entityHash(k), loaded.info().getEntities(k.ordinal()).getEntitySha256().toByteArray());
            }
            assertEquals(server.snapshot().chunkCount(), loaded.stats().chunksAccepted());
            assertEquals(1, server.metrics().transfersCompleted());
            log.info("TINY full load: {} records in {} ({})", loaded.stats().recordsAccepted(), loaded.stats().totalDuration(), loaded.stats().durationByPhase());
        }
    }

    @Test public void subsetLoadCarriesOnlyTheRequestedTypesPlusCountryAndCurrency() throws Exception {
        try (RefDataServer server = ItSupport.startServer(db)) {
            LoadedSnapshot loaded = ItSupport.client(ItSupport.endpoint(server))
                    .entityTypes(EntityKind.ACCOUNT, EntityKind.SETTLEMENT_INSTRUCTION).build().load();
            RefDataStore store = loaded.store();
            assertEquals(reference.countries(), store.countries());
            assertEquals(reference.currencies(), store.currencies());
            assertEquals(reference.accounts(), store.accounts());
            assertEquals(reference.settlementInstructions(), store.settlementInstructions());
            assertEquals(0, store.count(EntityKind.CLIENT));
            assertEquals(0, store.count(EntityKind.ADDRESS));
            long expectedChunks = 0;
            for (EntityKind k : new EntityKind[] {EntityKind.COUNTRY, EntityKind.CURRENCY, EntityKind.ACCOUNT, EntityKind.SETTLEMENT_INSTRUCTION}) {
                expectedChunks += server.snapshot().manifest(k).getChunkCount();
            }
            assertEquals(expectedChunks, loaded.stats().chunksAccepted());
            assertNotNull(store.account(reference.accounts().get(0).id()));
            assertNull("client of a subset-loaded account is absent", store.client(reference.accounts().get(0).clientId()));
        }
    }

    @Test public void lz4AndZstdPayloadsLoadEndToEnd() throws Exception {
        try (RefDataServer server = ItSupport.startServer(db, "rds.codecs", "LZ4,ZSTD")) {
            assertTrue(server.snapshot().supports(Codec.CODEC_LZ4));
            assertTrue(server.snapshot().supports(Codec.CODEC_ZSTD));
            for (Codec codec : new Codec[] {Codec.CODEC_LZ4, Codec.CODEC_ZSTD}) {
                LoadedSnapshot loaded = ItSupport.client(ItSupport.endpoint(server)).acceptedCodecs(codec).build().load();
                ItSupport.assertSameStore(reference, loaded.store());
                long plain = server.snapshot().totalFrameBytes(Codec.CODEC_NONE);
                long compressed = server.snapshot().totalFrameBytes(codec);
                assertTrue(codec + " frames should be smaller than plain", compressed < plain);
                assertTrue(codec + " bytes received " + loaded.stats().bytesReceived() + " vs plain " + plain, loaded.stats().bytesReceived() < plain);
                log.info("{}: {} bytes on the wire vs {} uncompressed, loaded in {}", codec, loaded.stats().bytesReceived(), plain, loaded.stats().totalDuration());
            }
        }
    }

    @Test public void secondLoadWithTheHeldSnapshotTransfersNothing() throws Exception {
        try (RefDataServer server = ItSupport.startServer(db)) {
            RefDataClient client = ItSupport.client(ItSupport.endpoint(server)).build();
            LoadedSnapshot first = client.load();
            LoadedSnapshot second = client.load(first);
            assertSame(first.store(), second.store());
            assertEquals(0, second.stats().chunksAccepted());
            assertTrue("HelloAck, SnapshotBegin and SnapshotEnd were still read", second.stats().bytesReceived() > 0);
            assertEquals(1, server.metrics().transfersCompleted());
        }
    }

    @Test public void orphanAccountFailsStartupUnderFailAndIsDroppedUnderQuarantine() throws Exception {
        long orphan;
        try (Connection c = db.connect()) {
            orphan = new BadDataInjector(c).orphanAccount();
        }
        StoreLoader.StartupException e = assertThrows(StoreLoader.StartupException.class,
                () -> ItSupport.startServer(db, "rds.validation.policy", "FAIL"));
        assertTrue(e.getMessage(), e.getMessage().contains("validation"));
        assertTrue(e.getMessage(), e.getMessage().contains("MISSING_REFERENCE"));

        try (RefDataServer server = ItSupport.startServer(db, "rds.validation.policy", "QUARANTINE")) {
            LoadedSnapshot loaded = ItSupport.client(ItSupport.endpoint(server)).build().load();
            assertNull("orphan account must not be published", loaded.store().account(orphan));
            assertEquals(reference.accounts().size() - 1, loaded.store().count(EntityKind.ACCOUNT));
            assertTrue("dependants of the orphan are dropped too", loaded.store().settlementInstructions().stream().noneMatch(s -> s.accountId() == orphan));
            assertEquals(reference.clients(), loaded.store().clients());
        }
    }
}
