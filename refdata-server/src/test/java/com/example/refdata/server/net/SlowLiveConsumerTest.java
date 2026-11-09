package com.example.refdata.server.net;

import com.example.refdata.contract.Framing;
import com.example.refdata.protocol.v1.Envelope;
import com.example.refdata.protocol.v1.ErrorCode;
import com.example.refdata.protocol.v1.Hello;
import com.example.refdata.protocol.v1.LiveResume;
import com.example.refdata.protocol.v1.SnapshotRequest;
import com.example.refdata.server.ServerConfig;
import com.example.refdata.server.TestStores;
import com.example.refdata.server.health.ServerMetrics;
import com.example.refdata.server.snapshot.Snapshot;
import com.example.refdata.server.snapshot.SnapshotEncoder;
import com.example.refdata.testing.IntegrationTest;
import com.google.protobuf.ByteString;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Milestone 10 over real TCP: a live consumer that stops reading is disconnected after {@code rds.live.max.lag}
 * without delaying publication or a healthy consumer, and a planned shutdown does not wait for it.
 */
@Category(IntegrationTest.class)
public class SlowLiveConsumerTest {
    private static final SnapshotEncoder ENCODER = new SnapshotEncoder(64 << 10, 128 << 10, Set.of());
    private static final long BASE = 1_000;
    /** ~19 MB of updates: far more than the 256 KiB high-water mark plus the loopback socket buffers. */
    private static final int UPDATES = 160;
    private static final int UPDATE_BYTES = 120 << 10;

    private ServerMetrics metrics;
    private RefDataTcpServer server;
    private final List<Snapshot> versions = new ArrayList<>();

    @Before public void start() throws Exception {
        Properties p = new Properties();
        p.setProperty("rds.tcp.port", "0");
        p.setProperty("rds.tcp.bind.address", "127.0.0.1");
        p.setProperty("rds.io.threads", "2");
        p.setProperty("rds.chunk.target.bytes", "64k");
        p.setProperty("rds.frame.max.bytes", "128k");
        p.setProperty("rds.write.buffer.high.water", "256k");
        p.setProperty("rds.write.buffer.low.water", "128k");
        p.setProperty("rds.tcp.send.buffer", "64k");
        p.setProperty("rds.live.max.lag", "300ms");
        p.setProperty("rds.live.history.batches", String.valueOf(UPDATES));
        p.setProperty("rds.live.history.bytes", "32m");
        p.setProperty("rds.shutdown.drain.timeout", "10s");
        metrics = new ServerMetrics();
        server = new RefDataTcpServer(ServerConfig.fromProperties(p), ENCODER.encode(TestStores.small(BASE), "test", 0), metrics, "slow", "0");
        server.start();
        for (int i = 1; i <= UPDATES; i++) versions.add(ENCODER.encode(TestStores.small(BASE + i), "test", 0));   // encode outside the timing
    }

    @After public void stop() { server.shutdown(); }

    /** An update body that names its version and is padded to {@link #UPDATE_BYTES}. */
    private static Envelope update(long version) {
        ByteBuffer b = ByteBuffer.allocate(UPDATE_BYTES).putLong(version);
        return Envelope.newBuilder().setUpdateBatch(ByteString.copyFrom(b.array())).build();
    }

    private static long versionOf(Envelope e) {
        return e.getUpdateBatch().asReadOnlyByteBuffer().getLong();
    }

    /** Connects, subscribes to live updates and resumes at {@link #BASE} (no snapshot transfer needed). */
    private RawClient subscriber(String name, int receiveBuffer) throws Exception {
        RawClient c = new RawClient(server.port(), 10_000, receiveBuffer);
        c.send(Envelope.newBuilder().setHello(Hello.newBuilder().setProtocolMajor(1).addSchemaMajors(1).setClientName(name)
                .setKeepAlive(true).setLiveUpdates(true).setMaxFrameBytes(Framing.DEFAULT_MAX_FRAME_BYTES)).build());
        Envelope ack = c.readSkippingHeartbeats();
        assertTrue(ack.toString(), ack.getHelloAck().getLiveUpdates());
        c.send(Envelope.newBuilder().setSnapshotRequest(SnapshotRequest.newBuilder().setRequestId(1)
                .setLiveResume(LiveResume.newBuilder().setGeneration(ack.getHelloAck().getUpdateGeneration()).setDataVersion(BASE))).build());
        assertEquals(Envelope.BodyCase.UPDATES_RESUMED, c.readSkippingHeartbeats().getBodyCase());
        return c;
    }

    private void publishAll() {
        for (int i = 1; i <= UPDATES; i++) server.publishUpdate(versions.get(i - 1), update(BASE + i));
    }

    @Test public void stalledConsumerIsDisconnectedWhileTheHealthyOneGetsEveryUpdate() throws Exception {
        try (RawClient stalled = subscriber("stalled", 32 << 10); RawClient healthy = subscriber("healthy", 0)) {
            waitFor(() -> metrics.liveSessionsActive() == 2);
            CompletableFuture<List<Long>> received = CompletableFuture.supplyAsync(() -> {
                List<Long> got = new ArrayList<>();
                try {
                    while (got.size() < UPDATES) {
                        Envelope e = healthy.readSkippingHeartbeats();
                        assertNotNull("healthy consumer disconnected after " + got.size() + " updates", e);
                        assertEquals(Envelope.BodyCase.UPDATE_BATCH, e.getBodyCase());
                        got.add(versionOf(e));
                    }
                } catch (Exception ex) { throw new RuntimeException(ex); }
                return got;
            });

            long t0 = System.nanoTime();
            publishAll();
            long publishMs = (System.nanoTime() - t0) / 1_000_000;
            assertTrue("publication must not wait for consumers, took " + publishMs + " ms", publishMs < 2_000);

            List<Long> got = received.get(20, TimeUnit.SECONDS);
            for (int i = 0; i < UPDATES; i++) assertEquals("healthy consumer, update " + i, BASE + i + 1, (long) got.get(i));

            waitFor(() -> metrics.liveResyncs("slow_consumer") == 1);
            waitFor(() -> metrics.liveSessionsActive() == 1 && metrics.liveSessionsLagging() == 0);

            // the stalled consumer received an in-order prefix of the updates; its stream then ends (error frame best effort)
            long expected = BASE + 1;
            Envelope e, last = null;
            while ((e = stalled.readSkippingHeartbeats()) != null) {
                last = e;
                if (e.getBodyCase() == Envelope.BodyCase.UPDATE_BATCH) assertEquals(expected++, versionOf(e));
            }
            assertTrue("the stalled consumer must not have received everything", expected <= BASE + UPDATES);
            if (last != null && last.getBodyCase() == Envelope.BodyCase.ERROR) {
                assertEquals(ErrorCode.ERROR_CODE_SLOW_CONSUMER, last.getError().getCode());
                assertTrue(last.getError().getFatal());
            }
            assertEquals(server.liveHistory().size(), UPDATES);        // the history still lets it resume from where it stopped
            assertTrue(server.canResumeFrom(expected - 1));
        }
        waitFor(() -> metrics.liveSessionsActive() == 0);
    }

    @Test public void shutdownDoesNotWaitForAStalledLiveConsumer() throws Exception {
        try (RawClient stalled = subscriber("stalled", 32 << 10)) {
            waitFor(() -> metrics.liveSessionsActive() == 1);
            publishAll();
            waitFor(() -> metrics.liveSessionsLagging() == 1);
            long t0 = System.nanoTime();
            server.shutdown();
            long ms = (System.nanoTime() - t0) / 1_000_000;
            assertTrue("shutdown took " + ms + " ms; it must not use the 10 s drain for a live session", ms < 8_000);
            try { while (stalled.readSkippingHeartbeats() != null) { /* drain until the server's close */ } }
            catch (java.io.IOException cutMidFrame) { /* expected: the close may truncate a buffered frame */ }
        }
        assertEquals(0, metrics.liveSessionsActive());
    }

    private static void waitFor(java.util.function.BooleanSupplier cond) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (!cond.getAsBoolean() && System.currentTimeMillis() < deadline) Thread.sleep(20);
        assertTrue("condition not met in time", cond.getAsBoolean());
    }
}
