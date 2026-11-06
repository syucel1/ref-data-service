package com.example.refdata.client;

import com.example.refdata.contract.ChangeCodec;
import com.example.refdata.model.ChangeBatch;
import com.example.refdata.model.Country;
import com.example.refdata.model.EntityKind;
import com.example.refdata.protocol.v1.Envelope;
import com.google.protobuf.ByteString;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.List;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Milestone 9 in the client: reconnect with the last applied version, replay, skip duplicates, reload after gaps. */
public class LiveResumeTest {
    private static final ByteString GENERATION = ByteString.copyFromUtf8("generation-a");
    private final SnapshotFixture fixture = SnapshotFixture.sample();   // data version 7
    private FakeServer server;

    @Before public void setUp() throws Exception { server = new FakeServer(); }
    @After public void tearDown() throws Exception { server.close(); }

    private RefDataClient liveClient() {
        return RefDataClient.builder().clientName("live-test").clientInstance("junit").endpoints(server.endpoint()).liveUpdates(true)
                .backoff(Duration.ofMillis(1), Duration.ofMillis(5)).connectTimeout(Duration.ofSeconds(2))
                .readTimeout(Duration.ofSeconds(3)).startupTimeout(Duration.ofSeconds(20)).build();
    }

    /** Renames GB: moves the data from {@code base} to {@code base + 1}. */
    private static Envelope rename(long base) throws java.io.IOException {
        Country gb = new Country("GB", "GBR", 826, "United Kingdom v" + (base + 1), "EMEA");
        ChangeBatch batch = new ChangeBatch(base, base + 1, List.of(new ChangeBatch.Change(EntityKind.COUNTRY, ChangeBatch.Operation.UPDATE, "GB", gb)));
        return Envelope.newBuilder().setUpdateBatch(ByteString.copyFrom(ChangeCodec.encode(batch))).build();
    }

    private static void await(String what, BooleanSupplier condition) throws InterruptedException {
        long end = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > end) throw new AssertionError("timed out waiting for " + what);
            Thread.sleep(5);
        }
    }

    @Test public void droppedConnectionResumesFromTheLastAppliedVersionAndSkipsDuplicates() throws Exception {
        server.onConnection(new SnapshotScript(fixture).live(GENERATION).afterEnd(s -> {
            s.send(rename(7));
            s.send(rename(8));
            Thread.sleep(200);                                          // let the client apply both, then drop the connection
        }));
        SnapshotScript resumed = new SnapshotScript(fixture).live(GENERATION).acceptLiveResume().afterEnd(s -> {
            s.send(rename(8));                                          // a duplicate of what the client already holds
            s.send(rename(9));
            s.drain();
        });
        server.onConnection(resumed);
        try (RefDataClient client = liveClient()) {
            assertEquals(7, client.load().store().dataVersion());
            await("data version 10 after the resume", () -> client.currentStore().dataVersion() == 10);
            assertEquals("United Kingdom v10", client.currentStore().country("GB").name());
            assertTrue(resumed.request().hasLiveResume());
            assertEquals(GENERATION, resumed.request().getLiveResume().getGeneration());
            assertEquals(9, resumed.request().getLiveResume().getDataVersion());
            assertEquals(1, client.liveResumes());
            assertEquals(0, client.liveSnapshotReloads());
        }
        server.assertNoFailures();
    }

    @Test public void serverThatCannotReplayServesASnapshotOnTheSameRequest() throws Exception {
        server.onConnection(new SnapshotScript(fixture).live(GENERATION).afterEnd(s -> { s.send(rename(7)); Thread.sleep(200); }));
        SnapshotScript fallback = new SnapshotScript(fixture).live(GENERATION);    // history expired: ignores live_resume
        server.onConnection(fallback);
        try (RefDataClient client = liveClient()) {
            client.load();
            await("the fallback snapshot", () -> client.liveSnapshotReloads() == 1);
            assertTrue("the client asked to resume", fallback.request().hasLiveResume());
            assertEquals(8, fallback.request().getLiveResume().getDataVersion());
            assertEquals("back to the snapshot's version", 7, client.currentStore().dataVersion());
            assertEquals(0, client.liveResumes());
            assertEquals(2, server.connections());
        }
        server.assertNoFailures();
    }

    @Test public void otherUpdateGenerationLoadsASnapshotWithoutAskingToResume() throws Exception {
        server.onConnection(new SnapshotScript(fixture).live(GENERATION).afterEnd(s -> Thread.sleep(100)));
        SnapshotScript restarted = new SnapshotScript(fixture).live(ByteString.copyFromUtf8("generation-b")).acceptLiveResume();
        server.onConnection(restarted);
        try (RefDataClient client = liveClient()) {
            client.load();
            await("the snapshot from the restarted server", () -> client.liveSnapshotReloads() == 1);
            assertFalse(restarted.request().hasLiveResume());
            assertEquals(0, client.liveResumes());
        }
        server.assertNoFailures();
    }

    @Test public void updateGapForcesASnapshotReloadInsteadOfAResume() throws Exception {
        server.onConnection(new SnapshotScript(fixture).live(GENERATION).afterEnd(s -> {
            s.send(rename(9));                                          // the client is at 7: 8 is missing
            s.drain();
        }));
        SnapshotScript reload = new SnapshotScript(fixture).live(GENERATION).acceptLiveResume();
        server.onConnection(reload);
        try (RefDataClient client = liveClient()) {
            client.load();
            await("the reload after the gap", () -> client.liveSnapshotReloads() == 1);
            assertFalse("a failed update must not be resumed", reload.request().hasLiveResume());
            assertEquals(7, client.currentStore().dataVersion());
        }
        server.assertNoFailures();
    }

    @Test public void resumeIsRepeatedAcrossSeveralDisconnects() throws Exception {
        server.onConnection(new SnapshotScript(fixture).live(GENERATION).afterEnd(s -> { s.send(rename(7)); Thread.sleep(150); }));
        server.onConnection(new SnapshotScript(fixture).live(GENERATION).acceptLiveResume().afterEnd(s -> { s.send(rename(8)); Thread.sleep(150); }));
        SnapshotScript third = new SnapshotScript(fixture).live(GENERATION).acceptLiveResume().afterEnd(s -> { s.send(rename(9)); s.drain(); });
        server.onConnection(third);
        try (RefDataClient client = liveClient()) {
            client.load();
            await("data version 10", () -> client.currentStore().dataVersion() == 10);
            assertEquals(9, third.request().getLiveResume().getDataVersion());
            assertEquals(2, client.liveResumes());
            assertEquals(0, client.liveSnapshotReloads());
        }
        server.assertNoFailures();
    }
}
