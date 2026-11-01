package com.example.refdata.server.net;

import com.example.refdata.protocol.v1.Codec;
import com.example.refdata.protocol.v1.Envelope;
import com.example.refdata.protocol.v1.ErrorCode;
import com.example.refdata.protocol.v1.SnapshotRequest;
import com.example.refdata.server.ServerConfig;
import com.example.refdata.server.TestStores;
import com.example.refdata.server.health.ServerMetrics;
import com.example.refdata.server.snapshot.Snapshot;
import com.example.refdata.server.snapshot.SnapshotEncoder;
import com.example.refdata.testing.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** A snapshot much larger than the write buffer: the streamer must pause on the water mark and a stalled reader must be cut off. */
@Category(IntegrationTest.class)
public class BackPressureTest {
    /** ~13 MB of frames, far more than the 1 MiB high-water mark plus the loopback socket buffers. */
    private static final Snapshot SNAPSHOT = new SnapshotEncoder(256 << 10, 1 << 20, Set.of()).encode(TestStores.sized(40_000), "test", 0);

    private ServerMetrics metrics;
    private RefDataTcpServer server;

    @Before public void start() throws Exception {
        Properties p = new Properties();
        p.setProperty("rds.tcp.port", "0");
        p.setProperty("rds.tcp.bind.address", "127.0.0.1");
        p.setProperty("rds.io.threads", "1");
        p.setProperty("rds.write.buffer.high.water", "1m");
        p.setProperty("rds.write.buffer.low.water", "512k");
        p.setProperty("rds.stall.timeout", "500ms");
        p.setProperty("rds.heartbeat.interval", "100ms");
        p.setProperty("rds.shutdown.drain.timeout", "10s");
        metrics = new ServerMetrics();
        server = new RefDataTcpServer(ServerConfig.fromProperties(p), SNAPSHOT, metrics, "bp", "0");
        server.start();
    }

    @After public void stop() { server.shutdown(); }

    @Test public void slowReaderStillGetsEveryChunkInOrder() throws Exception {
        try (RawClient c = new RawClient(server.port(), 10_000)) {
            c.handshake("slow");
            c.send(Envelope.newBuilder().setSnapshotRequest(SnapshotRequest.newBuilder().setRequestId(1)).build());
            Thread.sleep(300);                      // let the server hit the high-water mark and pause
            assertEquals(Envelope.BodyCase.SNAPSHOT_BEGIN, c.read().getBodyCase());
            int seq = 0;
            Envelope e;
            while ((e = c.read()).getBodyCase() == Envelope.BodyCase.SNAPSHOT_CHUNK) {
                assertEquals(seq++, e.getSnapshotChunk().getSeq());
                assertEquals(Codec.CODEC_NONE, e.getSnapshotChunk().getCodec());
                if (seq % 10 == 0) Thread.sleep(5);   // slower than the server, well inside the stall timeout
            }
            assertEquals(Envelope.BodyCase.SNAPSHOT_END, e.getBodyCase());
            assertEquals(SNAPSHOT.chunkCount(), seq);
            assertEquals(SNAPSHOT.chunkCount(), e.getSnapshotEnd().getChunksSent());
        }
        waitFor(() -> metrics.transfersCompleted() == 1);
    }

    @Test public void shutdownMidStreamLetsTheTransferFinishThenSendsGoAway() throws Exception {
        try (RawClient c = new RawClient(server.port(), 10_000)) {
            c.handshake("draining");
            c.send(Envelope.newBuilder().setSnapshotRequest(SnapshotRequest.newBuilder().setRequestId(1)).build());
            assertEquals(Envelope.BodyCase.SNAPSHOT_BEGIN, c.read().getBodyCase());
            Thread shutdown = new Thread(server::shutdown, "test-shutdown");
            shutdown.start();                        // GoAway must not overtake the remaining chunks
            int seq = 0;
            Envelope e;
            while ((e = c.read()).getBodyCase() == Envelope.BodyCase.SNAPSHOT_CHUNK) {
                assertEquals(seq++, e.getSnapshotChunk().getSeq());
                if (seq % 10 == 0) Thread.sleep(5);
            }
            assertEquals(Envelope.BodyCase.SNAPSHOT_END, e.getBodyCase());
            assertEquals(SNAPSHOT.chunkCount(), seq);
            Envelope bye = c.read();
            assertNotNull(bye);
            assertEquals(Envelope.BodyCase.GO_AWAY, bye.getBodyCase());
            assertEquals(null, c.read());
            shutdown.join(15_000);
        }
        assertEquals(1, metrics.transfersCompleted());
    }

    @Test public void stalledReaderIsClosedWithSlowConsumer() throws Exception {
        try (RawClient c = new RawClient(server.port(), 10_000)) {
            c.handshake("stalled");
            c.send(Envelope.newBuilder().setSnapshotRequest(SnapshotRequest.newBuilder().setRequestId(1)).build());
            waitFor(() -> metrics.transfersFailed("stall") == 1);   // nothing read: no write progress after the buffers fill
            Envelope last = null, e;
            while ((e = c.read()) != null) last = e;                 // drain what was buffered; the stream ends
            assertNotNull(last);
            if (last.getBodyCase() == Envelope.BodyCase.ERROR) {      // best effort: the error frame may not make it through
                assertEquals(ErrorCode.ERROR_CODE_SLOW_CONSUMER, last.getError().getCode());
                assertTrue(last.getError().getFatal());
            }
            assertTrue(last.getBodyCase() != Envelope.BodyCase.SNAPSHOT_END);
        }
        waitFor(() -> metrics.sessionsActive() == 0);
        assertEquals(0, server.admission().active());
    }

    private static void waitFor(java.util.function.BooleanSupplier cond) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (!cond.getAsBoolean() && System.currentTimeMillis() < deadline) Thread.sleep(20);
        assertTrue("condition not met in time", cond.getAsBoolean());
    }
}
