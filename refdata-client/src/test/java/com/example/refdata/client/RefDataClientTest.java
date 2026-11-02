package com.example.refdata.client;

import com.example.refdata.contract.Digests;
import com.example.refdata.model.EntityKind;
import com.example.refdata.model.RefDataStore;
import com.example.refdata.protocol.v1.Codec;
import com.example.refdata.protocol.v1.Envelope;
import com.example.refdata.protocol.v1.ErrorCode;
import com.example.refdata.protocol.v1.ErrorReply;
import com.google.protobuf.ByteString;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** Drives {@link RefDataClient} against scripted {@link FakeServer}s: the happy path and every recovery rule of 13.3 it implements. */
public class RefDataClientTest {
    @Test public void inspectionFollowsLoadsAndClosesWithClient() throws Exception {
        a.onConnection(new SnapshotScript(fixture));
        int port;
        try (RefDataClient rds = client(a).inspection(new com.example.refdata.model.inspection.InspectionServer.Options(0)).build()) {
            port = rds.inspectionPort();
            var http = java.net.http.HttpClient.newHttpClient();
            var request = java.net.http.HttpRequest.newBuilder(java.net.URI.create("http://127.0.0.1:" + port)).build();
            assertEquals(503, http.send(request, java.net.http.HttpResponse.BodyHandlers.ofString()).statusCode());
            LoadedSnapshot loaded = rds.load();
            String body = http.send(request, java.net.http.HttpResponse.BodyHandlers.ofString()).body();
            assertTrue(body.contains(Digests.hex(loaded.info().getSnapshotId().toByteArray())));
            assertTrue(body.contains("COUNTRY</a>: 2"));
            a.onConnection(new SnapshotScript(fixture));
            rds.load();
            assertEquals(200, http.send(request, java.net.http.HttpResponse.BodyHandlers.ofString()).statusCode());
        }
        try (java.net.Socket ignored = new java.net.Socket("127.0.0.1", port)) {
            throw new AssertionError("inspection port should be closed");
        } catch (java.io.IOException expected) { /* closed */ }
    }
    private final SnapshotFixture fixture = SnapshotFixture.sample();
    private FakeServer a;
    private FakeServer b;

    @Before public void start() throws IOException {
        a = new FakeServer();
        b = new FakeServer();
    }

    @After public void stop() throws Exception {
        a.assertNoFailures();
        b.assertNoFailures();
        a.close();
        b.close();
    }

    private RefDataClient.Builder client(FakeServer... servers) {
        RefDataClient.Builder builder = RefDataClient.builder().clientName("test").clientInstance("junit")
                .backoff(Duration.ofMillis(1), Duration.ofMillis(5)).connectTimeout(Duration.ofSeconds(2))
                .readTimeout(Duration.ofSeconds(3)).startupTimeout(Duration.ofSeconds(20));
        for (FakeServer s : servers) builder.endpoints(s.endpoint());
        return builder;
    }

    private static void assertSample(RefDataStore store) {
        assertEquals(2, store.count(EntityKind.COUNTRY));
        assertEquals(2, store.count(EntityKind.CURRENCY));
        assertEquals(3, store.count(EntityKind.CLIENT));
        assertEquals(3, store.count(EntityKind.ADDRESS));
        assertEquals(3, store.count(EntityKind.ACCOUNT));
        assertEquals(3, store.count(EntityKind.SETTLEMENT_INSTRUCTION));
        assertEquals("United Kingdom", store.country("GB").name());
        assertEquals("Alpha Capital Ltd", store.client(3L).legalName());
        assertSame(store.client(1L), store.parentOf(store.client(3L)));
        assertEquals(101L, store.accountByNumber("ACC-101").id());
        assertEquals(1000L, store.defaultSsi(100L, "GBP", com.example.refdata.model.AssetClass.CASH, 18500).id());
        assertNull(store.client(99L));
    }

    @Test public void happyPathLoadsAndVerifiesTheSnapshot() {
        SnapshotScript script = new SnapshotScript(fixture);
        a.onConnection(script);
        LoadedSnapshot loaded = client(a).build().load();
        assertSample(loaded.store());
        assertEquals(7L, loaded.store().dataVersion());
        assertArrayEquals(fixture.snapshotId(), loaded.info().getSnapshotId().toByteArray());
        assertEquals(1L, script.request().getRequestId());
        assertTrue("unfiltered request sends no entity types", script.request().getEntityTypesList().isEmpty());
        assertEquals("test", script.hello().getClientName());
        assertEquals(SnapshotLoader.LIBRARY_VERSION, script.hello().getLibraryVersion());
        LoadStats stats = loaded.stats();
        assertEquals(fixture.chunks().size(), stats.chunksAccepted());
        assertEquals(16, stats.recordsAccepted());
        assertEquals(1, stats.connections());
        assertFalse(stats.queued());
        assertEquals(0, stats.resumes());
        assertEquals(0, stats.fullRestarts());
        assertTrue(stats.bytesReceived() > 0);
        assertTrue(stats.durationByPhase().containsKey(LoadPhase.STREAMING));
        assertTrue(stats.durationByPhase().containsKey(LoadPhase.INDEXING));
    }

    @Test public void loadAsyncCompletesWithTheSnapshot() throws Exception {
        a.onConnection(new SnapshotScript(fixture));
        assertSample(client(a).build().loadAsync().get().store());
    }

    @Test public void filteredRequestAddsCountryAndCurrencyAndLoadsOnlyThoseTypes() {
        SnapshotScript script = new SnapshotScript(fixture);
        a.onConnection(script);
        LoadedSnapshot loaded = client(a).entityTypes(EntityKind.ACCOUNT).build().load();
        assertEquals(List.of(1, 2, 5), script.request().getEntityTypesList());
        RefDataStore store = loaded.store();
        assertEquals(2, store.count(EntityKind.COUNTRY));
        assertEquals(2, store.count(EntityKind.CURRENCY));
        assertEquals(0, store.count(EntityKind.CLIENT));
        assertEquals(3, store.count(EntityKind.ACCOUNT));
        assertEquals(0, store.count(EntityKind.SETTLEMENT_INSTRUCTION));
        assertEquals(4, loaded.stats().chunksAccepted());
        assertEquals(7, loaded.stats().recordsAccepted());
        assertNotNull(store.accountByNumber("ACC-100"));
    }

    @Test public void chunksOfAnUnknownEntityTypeAreSkipped() {
        SnapshotFixture withUnknown = SnapshotFixture.sampleBuilder().unknownTypeChunk(new byte[] {1, 2, 3}).unknownTypeChunk(new byte[] {4, 5}).build();
        a.onConnection(new SnapshotScript(withUnknown));
        LoadedSnapshot loaded = client(a).build().load();
        assertSample(loaded.store());
        assertEquals(2, loaded.stats().skippedUnknownTypeChunks());
        assertEquals(12, loaded.stats().chunksAccepted());
    }

    @Test public void crcFailureIsReportedAndTheTransferResumesOnTheOtherHost() {
        SnapshotScript bad = new SnapshotScript(fixture).faultAt(4, SnapshotScript.Fault.CORRUPT_CRC);
        SnapshotScript good = new SnapshotScript(fixture);
        a.onConnection(bad);
        b.onConnection(good);
        LoadedSnapshot loaded = client(a, b).build().load();
        assertSample(loaded.store());
        assertEquals(1, bad.afterFault().size());
        Envelope report = bad.afterFault().get(0);
        assertEquals(Envelope.BodyCase.ERROR, report.getBodyCase());
        assertEquals(ErrorCode.ERROR_CODE_INTEGRITY_FAILURE, report.getError().getCode());
        assertEquals(4, report.getError().getSeq());
        assertEquals(1L, report.getError().getRequestId());
        assertEquals(ByteString.copyFrom(fixture.snapshotId()), good.request().getResumeSnapshotId());
        assertEquals(4, good.request().getResumeFromSeq());
        assertEquals(1, loaded.stats().integrityFailures());
        assertEquals(1, loaded.stats().resumes());
        assertEquals(2, loaded.stats().connections());
        assertEquals(fixture.chunks().size(), loaded.stats().chunksAccepted());
    }

    @Test public void sameChunkFailingTwiceCountsAsAFullFailureAndRestartsFromZero() {
        a.onConnection(new SnapshotScript(fixture).faultAt(3, SnapshotScript.Fault.CORRUPT_CRC));
        b.onConnection(new SnapshotScript(fixture).faultAt(3, SnapshotScript.Fault.CORRUPT_CRC));
        SnapshotScript third = new SnapshotScript(fixture);
        a.onConnection(third);
        LoadedSnapshot loaded = client(a, b).build().load();
        assertSample(loaded.store());
        assertTrue("third request must be a full request", third.request().getResumeSnapshotId().isEmpty());
        assertEquals(2, loaded.stats().integrityFailures());
        assertEquals(1, loaded.stats().fullRestarts());
    }

    @Test public void droppedConnectionResumesWithTheSameFilterFromTheFirstUnacceptedSeq() {
        SnapshotScript dropped = new SnapshotScript(fixture).faultAt(3, SnapshotScript.Fault.DROP_CONNECTION);
        SnapshotScript resumed = new SnapshotScript(fixture);
        a.onConnection(dropped);
        b.onConnection(resumed);
        LoadedSnapshot loaded = client(a, b).entityTypes(EntityKind.CLIENT).build().load();
        assertEquals(List.of(1, 2, 3), dropped.request().getEntityTypesList());
        assertEquals(List.of(1, 2, 3), resumed.request().getEntityTypesList());
        assertEquals(3, resumed.request().getResumeFromSeq());
        assertEquals(ByteString.copyFrom(fixture.snapshotId()), resumed.request().getResumeSnapshotId());
        assertEquals(3, loaded.store().count(EntityKind.CLIENT));
        assertEquals(1, loaded.stats().resumes());
        assertEquals(0, loaded.stats().integrityFailures());
    }

    @Test public void connectionDroppedAfterTheLastChunkResumesPastTheEndAndCompletes() {
        a.onConnection(new SnapshotScript(fixture).dropBeforeEnd());
        SnapshotScript resumed = new SnapshotScript(fixture);
        b.onConnection(resumed);
        LoadedSnapshot loaded = client(a, b).build().load();
        assertSample(loaded.store());
        assertEquals(fixture.chunks().size(), resumed.request().getResumeFromSeq());
        assertEquals(ByteString.copyFrom(fixture.snapshotId()), resumed.request().getResumeSnapshotId());
        assertEquals(fixture.chunks().size(), loaded.stats().chunksAccepted());
        assertEquals(1, loaded.stats().resumes());
    }

    @Test public void goAwayMidTransferResumesElsewhere() {
        a.onConnection(new SnapshotScript(fixture).faultAt(6, SnapshotScript.Fault.GO_AWAY));
        SnapshotScript resumed = new SnapshotScript(fixture);
        b.onConnection(resumed);
        LoadedSnapshot loaded = client(a, b).build().load();
        assertSample(loaded.store());
        assertEquals(6, resumed.request().getResumeFromSeq());
        assertEquals(1, loaded.stats().resumes());
    }

    @Test public void differentCurrentSnapshotAfterReconnectForcesAFullRestart() {
        SnapshotFixture other = SnapshotFixture.sampleVariant(8);
        assertFalse(java.util.Arrays.equals(fixture.snapshotId(), other.snapshotId()));
        a.onConnection(new SnapshotScript(fixture).faultAt(5, SnapshotScript.Fault.DROP_CONNECTION));
        SnapshotScript full = new SnapshotScript(other);
        b.onConnection(full);
        LoadedSnapshot loaded = client(a, b).build().load();
        assertEquals(8L, loaded.store().dataVersion());
        assertEquals(3, loaded.store().count(EntityKind.COUNTRY));
        assertTrue(full.request().getResumeSnapshotId().isEmpty());
        assertEquals(1, loaded.stats().fullRestarts());
        assertEquals(0, loaded.stats().resumes());
    }

    @Test public void snapshotNotAvailableOnResumeRestartsFromZero() {
        SnapshotFixture other = SnapshotFixture.sampleVariant(9);
        a.onConnection(new SnapshotScript(fixture).faultAt(5, SnapshotScript.Fault.DROP_CONNECTION));
        // announces the old id (so the client tries to resume) but only serves the other snapshot
        SnapshotScript refuses = new SnapshotScript(other).announce(fixture.info());
        SnapshotScript full = new SnapshotScript(other);
        b.onConnection(refuses);
        b.onConnection(full);
        LoadedSnapshot loaded = client(a, b).build().load();
        assertEquals(9L, loaded.store().dataVersion());
        assertFalse(refuses.request().getResumeSnapshotId().isEmpty());
        assertTrue(full.request().getResumeSnapshotId().isEmpty());
        assertEquals(1, loaded.stats().fullRestarts());
    }

    @Test public void entityHashMismatchRestartsAndGivesUpAfterThreeFailures() {
        SnapshotFixture corrupt = fixture.withCorruptEntityHash(EntityKind.CLIENT);
        for (int i = 0; i < SnapshotLoader.MAX_FULL_FAILURES; i++) a.onConnection(new SnapshotScript(corrupt));
        SnapshotIntegrityException e = assertThrows(SnapshotIntegrityException.class, () -> client(a).build().load());
        assertTrue(e.getMessage(), e.getMessage().contains("CLIENT"));
        assertEquals(SnapshotLoader.MAX_FULL_FAILURES, a.connections());
        assertTrue(a.allScriptsUsed());
    }

    @Test public void entityHashMismatchOnceIsRecoveredByARestart() {
        a.onConnection(new SnapshotScript(fixture.withCorruptEntityHash(EntityKind.ADDRESS)));
        SnapshotScript good = new SnapshotScript(fixture);
        a.onConnection(good);
        LoadedSnapshot loaded = client(a).build().load();
        assertSample(loaded.store());
        assertTrue(good.request().getResumeSnapshotId().isEmpty());
        assertEquals(1, loaded.stats().fullRestarts());
    }

    @Test public void serverBusyIsRetriedAfterTheHint() {
        ErrorReply busy = ErrorReply.newBuilder().setCode(ErrorCode.ERROR_CODE_SERVER_BUSY).setMessage("full").setFatal(true).setRetryAfterMs(100).build();
        a.onConnection(new SnapshotScript(fixture).rejectHello(busy));
        a.onConnection(new SnapshotScript(fixture));
        long start = System.nanoTime();
        LoadedSnapshot loaded = client(a).build().load();
        assertSample(loaded.store());
        assertEquals(2, loaded.stats().connections());
        assertTrue("waited for the retry hint", System.nanoTime() - start >= Duration.ofMillis(100).toNanos());
    }

    @Test public void queuedRequestWaitsThroughHeartbeatsThenTransfers() {
        a.onConnection(new SnapshotScript(fixture).queued(5).heartbeatIntervalMs(20));
        LoadedSnapshot loaded = client(a).readTimeout(Duration.ofMillis(50)).build().load();
        assertSample(loaded.store());
        assertEquals(1, loaded.stats().connections());
        assertTrue(loaded.stats().queued());
        assertTrue(loaded.stats().durationByPhase().get(LoadPhase.QUEUED).toMillis() >= 80);
    }

    @Test public void unsupportedProtocolVersionIsFatal() {
        ErrorReply reject = ErrorReply.newBuilder().setCode(ErrorCode.ERROR_CODE_UNSUPPORTED_PROTOCOL_VERSION).setMessage("too old").setFatal(true)
                .setSupportedProtocolMin(2).setSupportedProtocolMax(3).addSupportedSchemaMajors(2).build();
        a.onConnection(new SnapshotScript(fixture).rejectHello(reject));
        UnsupportedVersionException e = assertThrows(UnsupportedVersionException.class, () -> client(a, b).build().load());
        assertEquals(2, e.reply().getSupportedProtocolMin());
        assertEquals(1, a.connections());
        assertEquals(0, b.connections());
    }

    @Test public void lz4PayloadsAreDecoded() {
        SnapshotFixture lz4 = SnapshotFixture.sampleBuilder().codec(Codec.CODEC_LZ4).build();
        assertEquals(Codec.CODEC_LZ4, lz4.chunks().get(0).getCodec());
        SnapshotScript script = new SnapshotScript(lz4);
        a.onConnection(script);
        assertSample(client(a).acceptedCodecs(Codec.CODEC_LZ4, Codec.CODEC_ZSTD).build().load().store());
        assertEquals(List.of(Codec.CODEC_LZ4, Codec.CODEC_ZSTD), script.hello().getAcceptedCodecsList());
    }

    @Test public void zstdPayloadsAreDecoded() {
        SnapshotFixture zstd = SnapshotFixture.sampleBuilder().codec(Codec.CODEC_ZSTD).build();
        a.onConnection(new SnapshotScript(zstd));
        assertSample(client(a).acceptedCodecs(Codec.CODEC_ZSTD).build().load().store());
    }

    @Test public void notModifiedReturnsTheSnapshotAlreadyHeld() {
        a.onConnection(new SnapshotScript(fixture));
        SnapshotScript second = new SnapshotScript(fixture);
        a.onConnection(second);
        RefDataClient client = client(a).build();
        LoadedSnapshot first = client.load();
        LoadedSnapshot again = client.load(first);
        assertEquals(ByteString.copyFrom(fixture.snapshotId()), second.request().getIfSnapshotId());
        assertSame(first.store(), again.store());
        assertEquals(0, again.stats().chunksAccepted());
    }

    @Test public void changedSnapshotIsSentDespiteIfSnapshotId() {
        a.onConnection(new SnapshotScript(fixture));
        SnapshotFixture changed = SnapshotFixture.sampleVariant(8);
        a.onConnection(new SnapshotScript(changed));
        RefDataClient client = client(a).build();
        LoadedSnapshot first = client.load();
        LoadedSnapshot again = client.load(first);
        assertEquals(8L, again.store().dataVersion());
        assertEquals(3, again.store().count(EntityKind.COUNTRY));
        assertEquals(changed.chunks().size(), again.stats().chunksAccepted());
    }

    @Test public void dataVersionBelowMinimumOnEveryHostFails() {
        a.onConnection(new SnapshotScript(fixture));
        b.onConnection(new SnapshotScript(fixture));
        DataVersionException e = assertThrows(DataVersionException.class, () -> client(a, b).minDataVersion(8).build().load());
        assertTrue(e.getMessage(), e.getMessage().contains("minimum 8"));
        assertEquals(1, a.connections());
        assertEquals(1, b.connections());
    }

    @Test public void expectedDataVersionSkipsHostsThatServeAnotherVersion() {
        a.onConnection(new SnapshotScript(SnapshotFixture.sampleBuilder().dataVersion(6).build()));
        b.onConnection(new SnapshotScript(fixture));
        LoadedSnapshot loaded = client(a, b).expectedDataVersion(7).build().load();
        assertEquals(7L, loaded.store().dataVersion());
        assertEquals(2, loaded.stats().connections());
    }

    @Test public void unreachableHostsFailWithTheStartupTimeoutNamingThePhase() throws IOException {
        int freePort;
        try (ServerSocket s = new ServerSocket(0)) { freePort = s.getLocalPort(); }
        long start = System.nanoTime();
        RefDataUnavailableException e = assertThrows(RefDataUnavailableException.class, () -> RefDataClient.builder().clientName("test")
                .endpoints("127.0.0.1:" + freePort).backoff(Duration.ofMillis(10), Duration.ofMillis(50))
                .startupTimeout(Duration.ofMillis(300)).build().load());
        assertEquals(LoadPhase.CONNECTING, e.phase());
        assertTrue(e.getMessage(), e.getMessage().contains("CONNECTING"));
        assertTrue(System.nanoTime() - start >= Duration.ofMillis(300).toNanos());
    }

    @Test public void silentServerWhileQueuedFailsWithTheQueuedPhase() {
        a.onConnection(session -> {
            session.read();
            session.send(Envelope.newBuilder().setHelloAck(com.example.refdata.protocol.v1.HelloAck.newBuilder()
                    .setProtocolMajor(1).setSchemaMajor(1).setHeartbeatIntervalMs(50).setCurrentSnapshot(fixture.info())).build());
            session.read();
            session.drain();
        });
        RefDataUnavailableException e = assertThrows(RefDataUnavailableException.class,
                () -> client(a).readTimeout(Duration.ofMillis(100)).startupTimeout(Duration.ofMillis(500)).build().load());
        assertEquals(LoadPhase.QUEUED, e.phase());
    }

    @Test public void keepAliveSendsClientHeartbeatsOnScheduleUntilClosed() throws Exception {
        int intervalMs = 200;
        int intervals = 4;
        java.util.concurrent.CountDownLatch done = new java.util.concurrent.CountDownLatch(1);
        java.util.List<Long> gaps = java.util.Collections.synchronizedList(new java.util.ArrayList<>());
        a.onConnection(new SnapshotScript(fixture).heartbeatIntervalMs(intervalMs).afterEnd(session -> {
            long last = System.nanoTime();
            for (int i = 0; i < intervals; i++) {
                // the server heartbeats too: that alone must not keep the connection alive
                session.send(Envelope.newBuilder().setHeartbeat(com.example.refdata.protocol.v1.Heartbeat.newBuilder().setSentAtEpochMs(i)).build());
                assertEquals(Envelope.BodyCase.HEARTBEAT, session.read().getBodyCase());
                long now = System.nanoTime();
                gaps.add((now - last) / 1_000_000);
                last = now;
            }
            done.countDown();
            session.drain();
        }));
        try (RefDataClient client = client(a).keepAlive(true).build()) {
            LoadedSnapshot loaded = client.load();
            assertSample(loaded.store());
            assertTrue("client heartbeats keep arriving", done.await(5, java.util.concurrent.TimeUnit.SECONDS));
        }
        for (long gap : gaps) assertTrue("heartbeat gap " + gap + " ms must stay under 1.5 intervals", gap < intervalMs * 3 / 2);
        assertEquals(1, a.connections());
    }

    @Test public void builderAlwaysAddsCountryAndCurrencyToASubset() {
        SnapshotScript script = new SnapshotScript(fixture);
        a.onConnection(script);
        client(a).entityTypes(EntityKind.SETTLEMENT_INSTRUCTION, EntityKind.ACCOUNT).build().load();
        assertEquals(List.of(1, 2, 5, 6), script.request().getEntityTypesList());
    }

    @Test public void snapshotIdInTheFixtureMatchesTheContractArithmetic() {
        List<byte[]> hashes = fixture.info().getEntitiesList().stream().map(m -> m.getEntitySha256().toByteArray()).toList();
        assertArrayEquals(Digests.contentSha256(hashes), fixture.info().getContentSha256().toByteArray());
    }
}
