package com.example.refdata.server.net;

import com.example.refdata.contract.Digests;
import com.example.refdata.contract.Framing;
import com.example.refdata.model.EntityKind;
import com.example.refdata.protocol.v1.Codec;
import com.example.refdata.protocol.v1.Envelope;
import com.example.refdata.protocol.v1.ErrorCode;
import com.example.refdata.protocol.v1.Hello;
import com.example.refdata.protocol.v1.SnapshotChunk;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** End-to-end over real sockets with a raw client; tiny timeouts so the slow paths run quickly. */
@Category(IntegrationTest.class)
public class RefDataTcpServerTest {
    private static final Snapshot SNAPSHOT = new SnapshotEncoder(4096, 4096 + Framing.CHUNK_HEADROOM_BYTES, Set.of(Codec.CODEC_ZSTD))
            .encode(TestStores.sized(300), "test", 0);

    private ServerConfig config;
    private ServerMetrics metrics;
    private RefDataTcpServer server;

    @Before public void start() throws Exception {
        Properties p = new Properties();
        p.setProperty("rds.tcp.port", "0");
        p.setProperty("rds.tcp.bind.address", "127.0.0.1");
        p.setProperty("rds.io.threads", "2");
        p.setProperty("rds.handshake.timeout", "300ms");
        p.setProperty("rds.session.idle.timeout", "2s");
        p.setProperty("rds.heartbeat.interval", "100ms");
        p.setProperty("rds.snapshot.queue.max.wait", "3s");
        p.setProperty("rds.max.connections", "3");
        p.setProperty("rds.max.concurrent.snapshots", "1");
        p.setProperty("rds.snapshot.queue.size", "4");
        p.setProperty("rds.shutdown.drain.timeout", "2s");
        config = ServerConfig.fromProperties(p);
        metrics = new ServerMetrics();
        server = new RefDataTcpServer(config, SNAPSHOT, metrics, "test-server", "0");
        server.start();
    }

    @After public void stop() {
        server.shutdown();
    }

    private RawClient client() throws Exception {
        return new RawClient(server.port(), 5_000);
    }

    private static Envelope request(long id, int... types) {
        SnapshotRequest.Builder r = SnapshotRequest.newBuilder().setRequestId(id);
        for (int t : types) r.addEntityTypes(t);
        return Envelope.newBuilder().setSnapshotRequest(r).build();
    }

    /** Reads chunks until SnapshotEnd, checking seq contiguity within the expected ranges and every CRC. */
    private static List<SnapshotChunk> readTransfer(RawClient c, List<Integer> expectedSeqs) throws Exception {
        List<SnapshotChunk> chunks = new ArrayList<>();
        Envelope e;
        while ((e = c.read()).getBodyCase() == Envelope.BodyCase.SNAPSHOT_CHUNK) {
            SnapshotChunk ch = e.getSnapshotChunk();
            assertEquals((int) expectedSeqs.get(chunks.size()), ch.getSeq());
            assertEquals(ch.getCrc32(), Digests.crc32(ch.getPayload().toByteArray()));
            chunks.add(ch);
        }
        assertEquals(Envelope.BodyCase.SNAPSHOT_END, e.getBodyCase());
        assertEquals(expectedSeqs.size(), chunks.size());
        assertEquals(expectedSeqs.size(), e.getSnapshotEnd().getChunksSent());
        return chunks;
    }

    private static List<Integer> seqs(EntityKind... kinds) {
        List<Integer> out = new ArrayList<>();
        for (EntityKind k : kinds) for (int i = 0; i < SNAPSHOT.manifest(k).getChunkCount(); i++) out.add(SNAPSHOT.manifest(k).getFirstSeq() + i);
        return out;
    }

    @Test public void happyPathFullSnapshot() throws Exception {
        try (RawClient c = client()) {
            Envelope ack = c.handshake("happy");
            assertEquals(Envelope.BodyCase.HELLO_ACK, ack.getBodyCase());
            assertEquals(Codec.CODEC_NONE, ack.getHelloAck().getCodec());
            assertEquals(SNAPSHOT.snapshotIdBytes(), ack.getHelloAck().getCurrentSnapshot().getSnapshotId());
            c.send(request(1));
            Envelope begin = c.read();
            assertEquals(Envelope.BodyCase.SNAPSHOT_BEGIN, begin.getBodyCase());
            assertEquals(SNAPSHOT.chunkCount(), readTransfer(c, seqs(EntityKind.values())).size());
        }
        waitFor(() -> metrics.sessionsActive() == 0);
        assertEquals(1, metrics.transfersCompleted());
        assertTrue(metrics.bytesSent() >= SNAPSHOT.totalFrameBytes(Codec.CODEC_NONE));
    }

    @Test public void compressedCodecNegotiated() throws Exception {
        try (RawClient c = client()) {
            c.send(Envelope.newBuilder().setHello(Hello.newBuilder().setProtocolMajor(1).addSchemaMajors(1).setClientName("z").addAcceptedCodecs(Codec.CODEC_LZ4).addAcceptedCodecs(Codec.CODEC_ZSTD)).build());
            assertEquals(Codec.CODEC_ZSTD, c.read().getHelloAck().getCodec());
            c.send(request(1, 3));
            c.read();
            for (SnapshotChunk ch : readTransfer(c, seqs(EntityKind.CLIENT))) assertEquals(Codec.CODEC_ZSTD, ch.getCodec());
        }
    }

    @Test public void filteredSnapshot() throws Exception {
        try (RawClient c = client()) {
            c.handshake("filtered");
            c.send(request(2, 6, 1, 42));
            Envelope begin = c.read();
            assertEquals(List.of(1, 6), begin.getSnapshotBegin().getEntityTypesList());
            readTransfer(c, seqs(EntityKind.COUNTRY, EntityKind.SETTLEMENT_INSTRUCTION));
        }
    }

    @Test public void notModified() throws Exception {
        try (RawClient c = client()) {
            c.handshake("nm");
            c.send(Envelope.newBuilder().setSnapshotRequest(SnapshotRequest.newBuilder().setRequestId(3).setIfSnapshotId(SNAPSHOT.snapshotIdBytes())).build());
            assertTrue(c.read().getSnapshotBegin().getNotModified());
            assertEquals(0, c.read().getSnapshotEnd().getChunksSent());
        }
    }

    @Test public void resumeFromSeq() throws Exception {
        int from = SNAPSHOT.manifest(EntityKind.ADDRESS).getFirstSeq() + 2;
        try (RawClient c = client()) {
            c.handshake("resume");
            c.send(Envelope.newBuilder().setSnapshotRequest(SnapshotRequest.newBuilder().setRequestId(4).setResumeSnapshotId(SNAPSHOT.snapshotIdBytes()).setResumeFromSeq(from)).build());
            Envelope begin = c.read();
            assertEquals(from, begin.getSnapshotBegin().getFirstSeq());
            List<Integer> expected = new ArrayList<>();
            for (int s = from; s < SNAPSHOT.chunkCount(); s++) expected.add(s);
            List<SnapshotChunk> chunks = new ArrayList<>();
            Envelope e;
            while ((e = c.read()).getBodyCase() == Envelope.BodyCase.SNAPSHOT_CHUNK) chunks.add(e.getSnapshotChunk());
            assertEquals(expected.size(), chunks.size());
            assertEquals(from, chunks.get(0).getSeq());
            assertEquals(SNAPSHOT.chunkCount(), e.getSnapshotEnd().getChunksSent());      // totals cover the whole transfer
            assertEquals(SNAPSHOT.totalRecords(), e.getSnapshotEnd().getRecordsSent());
        }
    }

    @Test public void wrongProtocolMajor() throws Exception {
        try (RawClient c = client()) {
            c.send(Envelope.newBuilder().setHello(Hello.newBuilder().setProtocolMajor(9).addSchemaMajors(1)).build());
            Envelope e = c.read();
            assertEquals(ErrorCode.ERROR_CODE_UNSUPPORTED_PROTOCOL_VERSION, e.getError().getCode());
            assertTrue(e.getError().getFatal());
            assertNull(c.read());
        }
    }

    @Test public void requestBeforeHello() throws Exception {
        try (RawClient c = client()) {
            c.send(request(1));
            Envelope e = c.read();
            assertEquals(ErrorCode.ERROR_CODE_PROTOCOL_VIOLATION, e.getError().getCode());
            assertNull(c.read());
        }
    }

    @Test public void oversizedInboundFrame() throws Exception {
        try (RawClient c = client()) {
            c.sendRaw(new byte[] {0x00, 0x10, 0x00, 0x01, 0x0a, 0x0b});   // length 1 MiB + 1
            Envelope e = c.read();
            assertEquals(ErrorCode.ERROR_CODE_FRAME_TOO_LARGE, e.getError().getCode());
            assertTrue(e.getError().getFatal());
            assertNull(c.read());
        }
    }

    @Test public void zeroLengthFrameAndGarbage() throws Exception {
        try (RawClient c = client()) {
            c.sendRaw(new byte[] {0, 0, 0, 0});
            assertEquals(ErrorCode.ERROR_CODE_PROTOCOL_VIOLATION, c.read().getError().getCode());
            assertNull(c.read());
        }
        try (RawClient c = client()) {
            c.sendRaw(new byte[] {0, 0, 0, 3, (byte) 0xff, (byte) 0xff, (byte) 0xff});
            assertEquals(ErrorCode.ERROR_CODE_PROTOCOL_VIOLATION, c.read().getError().getCode());
            assertNull(c.read());
        }
    }

    @Test public void connectionLimit() throws Exception {
        List<RawClient> open = new ArrayList<>();
        try {
            for (int i = 0; i < 3; i++) { RawClient c = client(); c.handshake("limit-" + i); open.add(c); }
            try (RawClient extra = client()) {
                extra.send(RawClient.hello("too-many", false));       // a Hello on a rejected connection is dropped, never acknowledged
                Envelope e = extra.read();
                assertEquals(ErrorCode.ERROR_CODE_SERVER_BUSY, e.getError().getCode());
                assertTrue(e.getError().getFatal());
                assertTrue(e.getError().getRetryAfterMs() > 0);
                assertNull(extra.read());
            }
            assertEquals(1, metrics.connectionsRejected());
            open.get(0).close();
            open.remove(0);
            waitFor(() -> server.openConnections() == 2);
            try (RawClient again = client()) {
                assertEquals(Envelope.BodyCase.HELLO_ACK, again.handshake("after").getBodyCase());
            }
        } finally {
            for (RawClient c : open) c.close();
        }
    }

    @Test public void handshakeTimeout() throws Exception {
        try (RawClient c = client()) {
            Envelope e = c.read();
            assertEquals(ErrorCode.ERROR_CODE_HANDSHAKE_TIMEOUT, e.getError().getCode());
            assertNull(c.read());
        }
    }

    @Test public void secondClientIsQueuedWhileTheFirstStreams() throws Exception {
        try (RawClient second = client()) {
            // a non-reading client would not hold the slot (the whole snapshot fits the 8 MiB write buffer),
            // so pin the slot by hand and check the queueing protocol
            assertEquals(AdmissionController.Outcome.ADMITTED, server.admission().request(() -> { }).outcome());
            second.handshake("queued");
            second.send(request(5));
            Envelope q = second.read();
            assertEquals(Envelope.BodyCase.SNAPSHOT_QUEUED, q.getBodyCase());
            assertEquals(1, q.getSnapshotQueued().getQueuePosition());
            assertEquals(3000, q.getSnapshotQueued().getMaxWaitMs());
            assertEquals(Envelope.BodyCase.HEARTBEAT, second.read().getBodyCase());
            assertEquals(1, metrics.sessionsQueued());
            server.admission().release();
            assertEquals(Envelope.BodyCase.SNAPSHOT_BEGIN, second.readSkippingHeartbeats().getBodyCase());
            readTransfer(second, seqs(EntityKind.values()));
            assertEquals(0, metrics.sessionsQueued());
        }
    }

    @Test public void queueWaitExpiresWithServerBusy() throws Exception {
        assertEquals(AdmissionController.Outcome.ADMITTED, server.admission().request(() -> { }).outcome());
        try (RawClient c = client()) {
            c.handshake("expire");
            c.send(request(5));
            assertEquals(Envelope.BodyCase.SNAPSHOT_QUEUED, c.read().getBodyCase());
            Envelope e = c.readSkippingHeartbeats();
            assertEquals(ErrorCode.ERROR_CODE_SERVER_BUSY, e.getError().getCode());
            assertFalse(e.getError().getFatal());
            assertEquals(3000, e.getError().getRetryAfterMs());
            server.admission().release();
            c.send(request(6));    // the session is still usable
            assertEquals(Envelope.BodyCase.SNAPSHOT_BEGIN, c.read().getBodyCase());
            readTransfer(c, seqs(EntityKind.values()));
        }
    }

    @Test public void unknownEnvelopeBody() throws Exception {
        try (RawClient c = client()) {
            c.handshake("unknown");
            c.sendRaw(new byte[] {0, 0, 0, 4, (byte) 0xe2, 0x06, 0x01, 0x00});   // field 100 (unknown), length 1, body 0x00
            Envelope e = c.read();
            assertEquals(ErrorCode.ERROR_CODE_UNSUPPORTED_MESSAGE, e.getError().getCode());
            assertFalse(e.getError().getFatal());
            c.send(request(7, 1));
            assertEquals(Envelope.BodyCase.SNAPSHOT_BEGIN, c.read().getBodyCase());
            readTransfer(c, seqs(EntityKind.COUNTRY));
        }
    }

    @Test public void shutdownSendsGoAway() throws Exception {
        try (RawClient c = client()) {
            c.handshake("bye");
            server.shutdown();
            Envelope e = c.read();
            assertNotNull(e);
            assertEquals(Envelope.BodyCase.GO_AWAY, e.getBodyCase());
            assertEquals(ErrorCode.ERROR_CODE_SERVER_SHUTTING_DOWN, e.getGoAway().getReason());
            assertNull(c.read());
        }
    }

    private static void waitFor(java.util.function.BooleanSupplier cond) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (!cond.getAsBoolean() && System.currentTimeMillis() < deadline) Thread.sleep(20);
        assertTrue("condition not met in time", cond.getAsBoolean());
    }
}
