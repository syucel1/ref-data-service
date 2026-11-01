package com.example.refdata.server.net;

import com.example.refdata.contract.Framing;
import com.example.refdata.model.EntityKind;
import com.example.refdata.protocol.v1.Codec;
import com.example.refdata.protocol.v1.Envelope;
import com.example.refdata.protocol.v1.ErrorCode;
import com.example.refdata.protocol.v1.ErrorReply;
import com.example.refdata.protocol.v1.Heartbeat;
import com.example.refdata.protocol.v1.Hello;
import com.example.refdata.protocol.v1.SnapshotChunk;
import com.example.refdata.protocol.v1.SnapshotRequest;
import com.example.refdata.server.ServerConfig;
import com.example.refdata.server.TestStores;
import com.example.refdata.server.health.ServerMetrics;
import com.example.refdata.server.snapshot.Snapshot;
import com.example.refdata.server.snapshot.SnapshotEncoder;
import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.TooLongFrameException;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** The state machine on an EmbeddedChannel: Envelopes in, Envelopes and pre-encoded chunk ByteBufs out. */
public class SessionHandlerTest {
    private static final Snapshot SNAPSHOT = new SnapshotEncoder(2048, 2048 + Framing.CHUNK_HEADROOM_BYTES, Set.of(Codec.CODEC_LZ4))
            .encode(TestStores.sized(60), "test", 0);

    private ServerConfig config;
    private AdmissionController admission;
    private ServerMetrics metrics;
    private boolean accepting = true;
    private final List<Integer> corruptReports = new ArrayList<>();
    private final SessionHandler.TransferGate gate = new SessionHandler.TransferGate() {
        @Override public boolean acceptingTransfers() { return accepting; }
        @Override public void storedFrameCorrupt(int seq, Codec codec) { corruptReports.add(seq); }
    };

    @Before public void setUp() {
        Properties p = new Properties();
        p.setProperty("rds.handshake.timeout", "100ms");
        p.setProperty("rds.session.idle.timeout", "200ms");
        p.setProperty("rds.heartbeat.interval", "50ms");
        p.setProperty("rds.stall.timeout", "300ms");
        p.setProperty("rds.snapshot.max.duration", "5s");
        p.setProperty("rds.snapshot.queue.max.wait", "400ms");
        p.setProperty("rds.max.concurrent.snapshots", "1");
        p.setProperty("rds.snapshot.queue.size", "1");
        config = ServerConfig.fromProperties(p);
        admission = new AdmissionController(config.maxConcurrentSnapshots, config.snapshotQueueSize);
        metrics = new ServerMetrics();
    }

    private EmbeddedChannel channel() {
        return new EmbeddedChannel(new SessionHandler(config, SNAPSHOT, admission, gate, metrics, "srv", "test"));
    }

    private static Envelope hello(boolean keepAlive, Codec... codecs) {
        Hello.Builder h = Hello.newBuilder().setProtocolMajor(1).addSchemaMajors(1).setClientName("unit").setClientInstance("i1").setLibraryVersion("t").setKeepAlive(keepAlive);
        for (Codec c : codecs) h.addAcceptedCodecs(c);
        return Envelope.newBuilder().setHello(h).build();
    }

    private static Envelope request(long id, int... types) {
        SnapshotRequest.Builder r = SnapshotRequest.newBuilder().setRequestId(id);
        for (int t : types) r.addEntityTypes(t);
        return Envelope.newBuilder().setSnapshotRequest(r).build();
    }

    private static Envelope env(EmbeddedChannel ch) {
        Object o = ch.readOutbound();
        assertTrue("expected an Envelope, got " + o, o instanceof Envelope);
        return (Envelope) o;
    }

    private static ErrorReply error(EmbeddedChannel ch) {
        Envelope e = env(ch);
        assertEquals(Envelope.BodyCase.ERROR, e.getBodyCase());
        return e.getError();
    }

    private static List<SnapshotChunk> chunks(EmbeddedChannel ch, int n) throws Exception {
        List<SnapshotChunk> out = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            Object o = ch.readOutbound();
            assertTrue("expected chunk " + i + ", got " + o, o instanceof ByteBuf);
            ByteBuf b = (ByteBuf) o;
            int len = b.readInt();
            byte[] body = new byte[len];
            b.readBytes(body);
            assertEquals(0, b.readableBytes());
            out.add(Envelope.parseFrom(body).getSnapshotChunk());
        }
        return out;
    }

    private EmbeddedChannel ready(Codec... codecs) {
        EmbeddedChannel ch = channel();
        ch.writeInbound(hello(false, codecs));
        Envelope ack = env(ch);
        assertEquals(Envelope.BodyCase.HELLO_ACK, ack.getBodyCase());
        return ch;
    }

    @Test public void helloAckCarriesHeartbeatSnapshotAndCodec() {
        EmbeddedChannel ch = channel();
        assertEquals(SessionHandler.State.AWAIT_HELLO, ch.pipeline().get(SessionHandler.class).state());
        ch.writeInbound(hello(false, Codec.CODEC_ZSTD, Codec.CODEC_LZ4));
        Envelope e = env(ch);
        assertEquals(Envelope.BodyCase.HELLO_ACK, e.getBodyCase());
        assertEquals(Codec.CODEC_LZ4, e.getHelloAck().getCodec());          // ZSTD not pre-encoded, LZ4 is
        EmbeddedChannel plain = channel();
        plain.writeInbound(hello(false, Codec.CODEC_ZSTD, Codec.CODEC_NONE, Codec.CODEC_LZ4));
        assertEquals("NONE listed before LZ4 wins", Codec.CODEC_NONE, env(plain).getHelloAck().getCodec());
        assertEquals(50, e.getHelloAck().getHeartbeatIntervalMs());
        assertEquals(SNAPSHOT.info(), e.getHelloAck().getCurrentSnapshot());
        assertEquals(Framing.PROTOCOL_MAJOR, e.getHelloAck().getProtocolMajor());
        assertEquals(Framing.SCHEMA_MAJOR, e.getHelloAck().getSchemaMajor());
        assertFalse(e.getHelloAck().getSessionId().isEmpty());
        assertEquals(SessionHandler.State.READY, ch.pipeline().get(SessionHandler.class).state());
        assertEquals(2, metrics.sessionsActive());
    }

    @Test public void wrongProtocolMajorIsFatal() {
        EmbeddedChannel ch = channel();
        ch.writeInbound(Envelope.newBuilder().setHello(Hello.newBuilder().setProtocolMajor(2).addSchemaMajors(1)).build());
        ErrorReply err = error(ch);
        assertEquals(ErrorCode.ERROR_CODE_UNSUPPORTED_PROTOCOL_VERSION, err.getCode());
        assertTrue(err.getFatal());
        assertEquals(1, err.getSupportedProtocolMin());
        assertEquals(1, err.getSupportedProtocolMax());
        assertFalse(ch.isOpen());
    }

    @Test public void protocolMajorZeroIsFatal() {
        EmbeddedChannel ch = channel();
        ch.writeInbound(Envelope.newBuilder().setHello(Hello.newBuilder().setProtocolMajor(0).addSchemaMajors(1)).build());
        assertEquals(ErrorCode.ERROR_CODE_UNSUPPORTED_PROTOCOL_VERSION, error(ch).getCode());
        assertFalse(ch.isOpen());
    }

    @Test public void noCommonSchemaMajorIsFatal() {
        EmbeddedChannel ch = channel();
        ch.writeInbound(Envelope.newBuilder().setHello(Hello.newBuilder().setProtocolMajor(1).addSchemaMajors(7)).build());
        ErrorReply err = error(ch);
        assertEquals(ErrorCode.ERROR_CODE_UNSUPPORTED_SCHEMA_VERSION, err.getCode());
        assertEquals(List.of(1), err.getSupportedSchemaMajorsList());
        assertFalse(ch.isOpen());
    }

    @Test public void clientFrameLimitBelowLargestChunkIsFatal() {
        EmbeddedChannel ch = channel();
        ch.writeInbound(Envelope.newBuilder().setHello(Hello.newBuilder().setProtocolMajor(1).addSchemaMajors(1).setMaxFrameBytes(100)).build());
        assertEquals(ErrorCode.ERROR_CODE_FRAME_TOO_LARGE, error(ch).getCode());
        assertFalse(ch.isOpen());
    }

    @Test public void requestBeforeHelloIsProtocolViolation() {
        EmbeddedChannel ch = channel();
        ch.writeInbound(request(1));
        ErrorReply err = error(ch);
        assertEquals(ErrorCode.ERROR_CODE_PROTOCOL_VIOLATION, err.getCode());
        assertTrue(err.getFatal());
        assertFalse(ch.isOpen());
    }

    @Test public void secondHelloIsProtocolViolation() {
        EmbeddedChannel ch = ready();
        ch.writeInbound(hello(false));
        assertEquals(ErrorCode.ERROR_CODE_PROTOCOL_VIOLATION, error(ch).getCode());
        assertFalse(ch.isOpen());
    }

    @Test public void unknownBodyIsUnsupportedMessageNonFatal() {
        EmbeddedChannel ch = ready();
        ch.writeInbound(Envelope.newBuilder().build());
        ErrorReply err = error(ch);
        assertEquals(ErrorCode.ERROR_CODE_UNSUPPORTED_MESSAGE, err.getCode());
        assertFalse(err.getFatal());
        assertTrue(ch.isOpen());
        assertEquals(SessionHandler.State.READY, ch.pipeline().get(SessionHandler.class).state());
    }

    @Test public void handshakeTimeout() {
        EmbeddedChannel ch = channel();
        ch.advanceTimeBy(150, TimeUnit.MILLISECONDS);
        ch.runScheduledPendingTasks();
        assertEquals(ErrorCode.ERROR_CODE_HANDSHAKE_TIMEOUT, error(ch).getCode());
        assertFalse(ch.isOpen());
    }

    @Test public void idleTimeoutInReadyWithoutKeepAlive() {
        EmbeddedChannel ch = ready();
        ch.advanceTimeBy(250, TimeUnit.MILLISECONDS);
        ch.runScheduledPendingTasks();
        assertEquals(ErrorCode.ERROR_CODE_IDLE_TIMEOUT, error(ch).getCode());
        assertFalse(ch.isOpen());
    }

    @Test public void keepAliveSessionGetsHeartbeatsAndDiesAfterThreeMisses() {
        EmbeddedChannel ch = channel();
        ch.writeInbound(hello(true));
        env(ch);
        ch.advanceTimeBy(60, TimeUnit.MILLISECONDS);
        ch.runScheduledPendingTasks();
        assertEquals(Envelope.BodyCase.HEARTBEAT, env(ch).getBodyCase());
        ch.writeInbound(Envelope.newBuilder().setHeartbeat(Heartbeat.newBuilder().setSentAtEpochMs(1)).build());
        ch.advanceTimeBy(120, TimeUnit.MILLISECONDS);   // 180 ms since Hello, 120 ms since the client's heartbeat
        ch.runScheduledPendingTasks();
        assertTrue("alive: the client's heartbeat reset the deadline", ch.isOpen());
        ch.advanceTimeBy(60, TimeUnit.MILLISECONDS);    // > 3 x 50 ms since the last inbound frame
        ch.runScheduledPendingTasks();
        assertFalse(ch.isOpen());
    }

    @Test public void fullSnapshotHappyPath() throws Exception {
        EmbeddedChannel ch = ready();
        ch.writeInbound(request(7));
        Envelope begin = env(ch);
        assertEquals(Envelope.BodyCase.SNAPSHOT_BEGIN, begin.getBodyCase());
        assertEquals(7, begin.getSnapshotBegin().getRequestId());
        assertFalse(begin.getSnapshotBegin().getNotModified());
        assertEquals(0, begin.getSnapshotBegin().getFirstSeq());
        assertEquals(List.of(1, 2, 3, 4, 5, 6), begin.getSnapshotBegin().getEntityTypesList());
        List<SnapshotChunk> chunks = chunks(ch, SNAPSHOT.chunkCount());
        for (int i = 0; i < chunks.size(); i++) { assertEquals(i, chunks.get(i).getSeq()); assertEquals(Codec.CODEC_NONE, chunks.get(i).getCodec()); }
        Envelope end = env(ch);
        assertEquals(Envelope.BodyCase.SNAPSHOT_END, end.getBodyCase());
        assertEquals(SNAPSHOT.chunkCount(), end.getSnapshotEnd().getChunksSent());
        assertEquals(SNAPSHOT.totalRecords(), end.getSnapshotEnd().getRecordsSent());
        assertEquals(SNAPSHOT.info().getContentSha256(), end.getSnapshotEnd().getContentSha256());
        assertNull(ch.readOutbound());
        assertEquals(SessionHandler.State.READY, ch.pipeline().get(SessionHandler.class).state());
        assertEquals(0, admission.active());
        assertEquals(1, metrics.transfersCompleted());
        assertEquals(SNAPSHOT.totalFrameBytes(Codec.CODEC_NONE), ch.pipeline().get(SessionHandler.class).bytesSent());
        ch.close();
        assertEquals(1, metrics.sessions("unit"));
    }

    @Test public void filteredSnapshotIgnoresUnknownTypesAndUsesChosenCodec() throws Exception {
        EmbeddedChannel ch = ready(Codec.CODEC_LZ4);
        ch.writeInbound(request(3, 5, 99, 1, 5));   // ACCOUNT, unknown, COUNTRY, duplicate
        Envelope begin = env(ch);
        assertEquals(List.of(1, 5), begin.getSnapshotBegin().getEntityTypesList());
        int countryChunks = SNAPSHOT.manifest(EntityKind.COUNTRY).getChunkCount();
        int accountChunks = SNAPSHOT.manifest(EntityKind.ACCOUNT).getChunkCount();
        List<SnapshotChunk> chunks = chunks(ch, countryChunks + accountChunks);
        int seq = SNAPSHOT.manifest(EntityKind.COUNTRY).getFirstSeq();
        for (int i = 0; i < countryChunks; i++) assertEquals(seq++, chunks.get(i).getSeq());
        seq = SNAPSHOT.manifest(EntityKind.ACCOUNT).getFirstSeq();
        for (int i = countryChunks; i < chunks.size(); i++) { assertEquals(seq++, chunks.get(i).getSeq()); assertEquals(Codec.CODEC_LZ4, chunks.get(i).getCodec()); }
        Envelope end = env(ch);
        assertEquals(countryChunks + accountChunks, end.getSnapshotEnd().getChunksSent());
        assertEquals(SNAPSHOT.manifest(EntityKind.COUNTRY).getRecordCount() + SNAPSHOT.manifest(EntityKind.ACCOUNT).getRecordCount(), end.getSnapshotEnd().getRecordsSent());
    }

    @Test public void notModified() {
        EmbeddedChannel ch = ready();
        ch.writeInbound(Envelope.newBuilder().setSnapshotRequest(SnapshotRequest.newBuilder().setRequestId(2).setIfSnapshotId(SNAPSHOT.snapshotIdBytes())).build());
        Envelope begin = env(ch);
        assertTrue(begin.getSnapshotBegin().getNotModified());
        Envelope end = env(ch);
        assertEquals(0, end.getSnapshotEnd().getChunksSent());
        assertEquals(0, end.getSnapshotEnd().getRecordsSent());
        assertNull(ch.readOutbound());
        assertEquals(0, metrics.transfersStarted());
    }

    @Test public void differentIfSnapshotIdSendsEverything() throws Exception {
        EmbeddedChannel ch = ready();
        ch.writeInbound(Envelope.newBuilder().setSnapshotRequest(SnapshotRequest.newBuilder().setRequestId(2).setIfSnapshotId(ByteString.copyFrom(new byte[16]))).build());
        assertFalse(env(ch).getSnapshotBegin().getNotModified());
        chunks(ch, SNAPSHOT.chunkCount());
        assertEquals(Envelope.BodyCase.SNAPSHOT_END, env(ch).getBodyCase());
    }

    @Test public void resumeFromSeqWithTotalsCoveringTheWholeTransfer() throws Exception {
        EmbeddedChannel ch = ready();
        int from = SNAPSHOT.manifest(EntityKind.CLIENT).getFirstSeq() + 1;
        ch.writeInbound(Envelope.newBuilder().setSnapshotRequest(SnapshotRequest.newBuilder().setRequestId(5).addEntityTypes(3).addEntityTypes(6)
                .setResumeSnapshotId(SNAPSHOT.snapshotIdBytes()).setResumeFromSeq(from)).build());
        Envelope begin = env(ch);
        assertEquals(from, begin.getSnapshotBegin().getFirstSeq());
        int clientChunks = SNAPSHOT.manifest(EntityKind.CLIENT).getChunkCount();
        int ssiChunks = SNAPSHOT.manifest(EntityKind.SETTLEMENT_INSTRUCTION).getChunkCount();
        List<SnapshotChunk> chunks = chunks(ch, clientChunks - 1 + ssiChunks);
        assertEquals(from, chunks.get(0).getSeq());
        assertEquals(SNAPSHOT.manifest(EntityKind.SETTLEMENT_INSTRUCTION).getFirstSeq(), chunks.get(clientChunks - 1).getSeq());
        Envelope end = env(ch);
        assertEquals(clientChunks + ssiChunks, end.getSnapshotEnd().getChunksSent());
        assertEquals(SNAPSHOT.manifest(EntityKind.CLIENT).getRecordCount() + SNAPSHOT.manifest(EntityKind.SETTLEMENT_INSTRUCTION).getRecordCount(), end.getSnapshotEnd().getRecordsSent());
    }

    @Test public void resumeOutsideRangesIsInvalidRequest() {
        EmbeddedChannel ch = ready();
        ch.writeInbound(Envelope.newBuilder().setSnapshotRequest(SnapshotRequest.newBuilder().setRequestId(5).addEntityTypes(1)
                .setResumeSnapshotId(SNAPSHOT.snapshotIdBytes()).setResumeFromSeq(SNAPSHOT.chunkCount() - 1)).build());
        ErrorReply err = error(ch);
        assertEquals(ErrorCode.ERROR_CODE_INVALID_REQUEST, err.getCode());
        assertEquals(5, err.getRequestId());
        assertFalse(err.getFatal());
        assertTrue(ch.isOpen());
    }

    @Test public void resumeOnePastTheLastRequestedChunkSendsOnlyBeginAndEnd() {
        EmbeddedChannel ch = ready();
        int endOfAccounts = SNAPSHOT.manifest(EntityKind.ACCOUNT).getFirstSeq() + SNAPSHOT.manifest(EntityKind.ACCOUNT).getChunkCount();
        ch.writeInbound(Envelope.newBuilder().setSnapshotRequest(SnapshotRequest.newBuilder().setRequestId(8).addEntityTypes(1).addEntityTypes(5)
                .setResumeSnapshotId(SNAPSHOT.snapshotIdBytes()).setResumeFromSeq(endOfAccounts)).build());
        Envelope begin = env(ch);
        assertEquals(Envelope.BodyCase.SNAPSHOT_BEGIN, begin.getBodyCase());
        assertEquals(endOfAccounts, begin.getSnapshotBegin().getFirstSeq());
        assertEquals(List.of(1, 5), begin.getSnapshotBegin().getEntityTypesList());
        Envelope end = env(ch);
        assertEquals(Envelope.BodyCase.SNAPSHOT_END, end.getBodyCase());
        assertEquals(SNAPSHOT.manifest(EntityKind.COUNTRY).getChunkCount() + SNAPSHOT.manifest(EntityKind.ACCOUNT).getChunkCount(), end.getSnapshotEnd().getChunksSent());
        assertEquals(SNAPSHOT.manifest(EntityKind.COUNTRY).getRecordCount() + SNAPSHOT.manifest(EntityKind.ACCOUNT).getRecordCount(), end.getSnapshotEnd().getRecordsSent());
        assertNull(ch.readOutbound());
        assertEquals(SessionHandler.State.READY, ch.pipeline().get(SessionHandler.class).state());
        assertEquals(0, admission.active());
        // one further past the end is still invalid
        ch.writeInbound(Envelope.newBuilder().setSnapshotRequest(SnapshotRequest.newBuilder().setRequestId(9).addEntityTypes(1).addEntityTypes(5)
                .setResumeSnapshotId(SNAPSHOT.snapshotIdBytes()).setResumeFromSeq(endOfAccounts + 1)).build());
        assertEquals(ErrorCode.ERROR_CODE_INVALID_REQUEST, error(ch).getCode());
    }

    @Test public void resumeOfUnknownSnapshotIsNotAvailable() {
        EmbeddedChannel ch = ready();
        ch.writeInbound(Envelope.newBuilder().setSnapshotRequest(SnapshotRequest.newBuilder().setRequestId(5)
                .setResumeSnapshotId(ByteString.copyFrom(new byte[16])).setResumeFromSeq(1)).build());
        assertEquals(ErrorCode.ERROR_CODE_SNAPSHOT_NOT_AVAILABLE, error(ch).getCode());
        assertTrue(ch.isOpen());
    }

    @Test public void requestIdZeroIsInvalid() {
        EmbeddedChannel ch = ready();
        ch.writeInbound(request(0));
        assertEquals(ErrorCode.ERROR_CODE_INVALID_REQUEST, error(ch).getCode());
        assertTrue(ch.isOpen());
    }

    @Test public void queueingAdmitsWhenASlotFreesAndRejectsSecondRequestMeanwhile() throws Exception {
        EmbeddedChannel first = ready();
        EmbeddedChannel second = ready();
        EmbeddedChannel third = ready();
        first.writeInbound(request(1));
        assertEquals(Envelope.BodyCase.SNAPSHOT_BEGIN, env(first).getBodyCase());   // EmbeddedChannel is always writable: all chunks are out
        chunks(first, SNAPSHOT.chunkCount());
        // the transfer finishes synchronously in an EmbeddedChannel, so hold the slot by hand to test the queue
        assertEquals(Envelope.BodyCase.SNAPSHOT_END, env(first).getBodyCase());
        assertEquals(AdmissionController.Outcome.ADMITTED, admission.request(() -> { }).outcome());
        second.writeInbound(request(2));
        Envelope queued = env(second);
        assertEquals(Envelope.BodyCase.SNAPSHOT_QUEUED, queued.getBodyCase());
        assertEquals(1, queued.getSnapshotQueued().getQueuePosition());
        assertEquals(400, queued.getSnapshotQueued().getMaxWaitMs());
        assertEquals(SessionHandler.State.QUEUED, second.pipeline().get(SessionHandler.class).state());
        assertEquals(1, metrics.sessionsQueued());
        second.writeInbound(request(3));
        ErrorReply err = error(second);
        assertEquals(ErrorCode.ERROR_CODE_INVALID_REQUEST, err.getCode());
        assertFalse(err.getFatal());
        second.advanceTimeBy(60, TimeUnit.MILLISECONDS);
        second.runScheduledPendingTasks();
        assertEquals(Envelope.BodyCase.HEARTBEAT, env(second).getBodyCase());   // heartbeats while queued
        third.writeInbound(request(4));                                          // queue is full (size 1)
        ErrorReply busy = error(third);
        assertEquals(ErrorCode.ERROR_CODE_SERVER_BUSY, busy.getCode());
        assertFalse(busy.getFatal());
        assertEquals("queue full: long hint", 400, busy.getRetryAfterMs());
        admission.release();                                                     // slot freed: second is admitted
        assertEquals(Envelope.BodyCase.SNAPSHOT_BEGIN, env(second).getBodyCase());
        chunks(second, SNAPSHOT.chunkCount());
        assertEquals(Envelope.BodyCase.SNAPSHOT_END, env(second).getBodyCase());
        assertEquals(0, metrics.sessionsQueued());
        assertEquals(0, admission.active());
    }

    @Test public void queueWaitExpiresWithServerBusy() {
        assertEquals(AdmissionController.Outcome.ADMITTED, admission.request(() -> { }).outcome());
        EmbeddedChannel ch = ready();
        ch.writeInbound(request(2));
        assertEquals(Envelope.BodyCase.SNAPSHOT_QUEUED, env(ch).getBodyCase());
        ch.advanceTimeBy(450, TimeUnit.MILLISECONDS);
        ch.runScheduledPendingTasks();
        Envelope e;
        do { e = env(ch); } while (e.getBodyCase() == Envelope.BodyCase.HEARTBEAT);
        assertEquals(ErrorCode.ERROR_CODE_SERVER_BUSY, e.getError().getCode());
        assertEquals(2, e.getError().getRequestId());
        assertEquals("wait expired: short hint", SessionHandler.RETRY_AFTER_WAIT_EXPIRED_MS, e.getError().getRetryAfterMs());
        assertTrue(ch.isOpen());
        assertEquals(SessionHandler.State.READY, ch.pipeline().get(SessionHandler.class).state());
        assertEquals(0, admission.queued());
        admission.release();
    }

    @Test public void closingWhileQueuedLeavesTheQueue() {
        assertEquals(AdmissionController.Outcome.ADMITTED, admission.request(() -> { }).outcome());
        EmbeddedChannel ch = ready();
        ch.writeInbound(request(2));
        assertEquals(Envelope.BodyCase.SNAPSHOT_QUEUED, env(ch).getBodyCase());
        ch.close();
        assertEquals(0, admission.queued());
        assertEquals(0, metrics.sessionsQueued());
        admission.release();
        assertEquals(0, admission.active());
    }

    @Test public void integrityFailureReportIsCheckedAndCounted() {
        EmbeddedChannel ch = ready();
        ch.writeInbound(Envelope.newBuilder().setError(ErrorReply.newBuilder().setCode(ErrorCode.ERROR_CODE_INTEGRITY_FAILURE).setSeq(1).setFatal(true)).build());
        assertNull(ch.readOutbound());
        assertTrue(ch.isOpen());
        assertEquals(1, metrics.integrityReports());
        assertTrue("stored frame is intact: no corruption report", corruptReports.isEmpty());
    }

    @Test public void confirmedCorruptionIsReportedToTheGateAndNoTransfersAreAdmitted() throws Exception {
        // a snapshot whose chunk 0 bytes are corrupted in place (the frame array is shared, so copy the snapshot first)
        Snapshot damaged = new SnapshotEncoder(2048, 2048 + Framing.CHUNK_HEADROOM_BYTES, Set.of()).encode(TestStores.sized(10), "test", 0);
        byte[] frame = damaged.chunk(0).frame(Codec.CODEC_NONE);
        frame[frame.length - 1] ^= 0x01;
        EmbeddedChannel ch = new EmbeddedChannel(new SessionHandler(config, damaged, admission, gate, metrics, "srv", "test"));
        ch.writeInbound(hello(false));
        env(ch);
        ch.writeInbound(Envelope.newBuilder().setError(ErrorReply.newBuilder().setCode(ErrorCode.ERROR_CODE_INTEGRITY_FAILURE).setSeq(0).setFatal(true)).build());
        assertEquals(List.of(0), corruptReports);
        accepting = false;
        EmbeddedChannel other = ready();
        other.writeInbound(request(1));
        ErrorReply err = error(other);
        assertEquals(ErrorCode.ERROR_CODE_SERVER_BUSY, err.getCode());
        assertEquals(0, metrics.transfersStarted());
    }

    @Test public void inboundFrameErrorsAreFatal() {
        EmbeddedChannel ch = ready();
        ch.pipeline().fireExceptionCaught(new DecoderException(new TooLongFrameException("too long")));
        ErrorReply err = error(ch);
        assertEquals(ErrorCode.ERROR_CODE_FRAME_TOO_LARGE, err.getCode());
        assertTrue(err.getFatal());
        assertFalse(ch.isOpen());
    }

    @Test public void goAwayWhileQueuedOrReadyClosesAtOnce() throws Exception {
        assertEquals(AdmissionController.Outcome.ADMITTED, admission.request(() -> { }).outcome());
        EmbeddedChannel ch = ready();
        ch.writeInbound(request(1));
        assertEquals(Envelope.BodyCase.SNAPSHOT_QUEUED, env(ch).getBodyCase());
        // while queued there is nothing to finish: GoAway at once
        ch.pipeline().get(SessionHandler.class).goAway(500);
        assertEquals(Envelope.BodyCase.GO_AWAY, env(ch).getBodyCase());
        assertFalse(ch.isOpen());
        admission.release();
        // a session that just streamed (EmbeddedChannel writes synchronously; the mid-stream case is BackPressureTest)
        EmbeddedChannel s = ready();
        s.writeInbound(request(2));
        assertEquals(Envelope.BodyCase.SNAPSHOT_BEGIN, env(s).getBodyCase());
        chunks(s, SNAPSHOT.chunkCount());
        assertEquals(Envelope.BodyCase.SNAPSHOT_END, env(s).getBodyCase());
        s.pipeline().get(SessionHandler.class).goAway(500);
        assertEquals(Envelope.BodyCase.GO_AWAY, env(s).getBodyCase());
        assertFalse(s.isOpen());
    }

    @Test public void goAwayClosesIdleSessionsAndLetsTransfersFinish() {
        EmbeddedChannel ch = ready();
        ch.pipeline().get(SessionHandler.class).goAway(1000);
        Envelope e = env(ch);
        assertEquals(Envelope.BodyCase.GO_AWAY, e.getBodyCase());
        assertEquals(ErrorCode.ERROR_CODE_SERVER_SHUTTING_DOWN, e.getGoAway().getReason());
        assertEquals(1000, e.getGoAway().getReconnectAfterMs());
        assertFalse(ch.isOpen());
    }
}
