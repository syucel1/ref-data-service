package com.example.refdata.client;

import com.example.refdata.contract.Framing;
import com.example.refdata.protocol.v1.Codec;
import com.example.refdata.protocol.v1.Envelope;
import com.example.refdata.protocol.v1.ErrorCode;
import com.example.refdata.protocol.v1.ErrorReply;
import com.example.refdata.protocol.v1.GoAway;
import com.example.refdata.protocol.v1.Heartbeat;
import com.example.refdata.protocol.v1.Hello;
import com.example.refdata.protocol.v1.HelloAck;
import com.example.refdata.protocol.v1.SnapshotBegin;
import com.example.refdata.protocol.v1.SnapshotChunk;
import com.example.refdata.protocol.v1.SnapshotInfo;
import com.example.refdata.protocol.v1.SnapshotQueued;
import com.example.refdata.protocol.v1.SnapshotRequest;
import com.example.refdata.protocol.v1.UpdatesResumed;
import com.google.protobuf.ByteString;

import java.io.EOFException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * A {@link FakeServer.ConnectionScript} that behaves like the real server for one connection (handshake, request,
 * filter, resume, not-modified, queueing) with optional faults injected at chosen points. Records what the client
 * sent so tests can assert on it.
 */
public final class SnapshotScript implements FakeServer.ConnectionScript {
    /** A fault raised at the chosen chunk. */
    public enum Fault { CORRUPT_CRC, DROP_CONNECTION, GO_AWAY }

    /** Close after the last chunk, before SnapshotEnd (use as {@code faultAt(anySeq, ...)} is not possible: no chunk is due). */
    private boolean dropBeforeEnd;

    private final SnapshotFixture fixture;
    private int heartbeatIntervalMs = 5_000;
    private ErrorReply handshakeError;
    private int queuedHeartbeats;
    private int faultSeq = -1;
    private Fault fault;
    private SnapshotInfo announcedSnapshot;
    private FakeServer.ConnectionScript afterEnd = FakeServer.Session::drain;
    private ByteString liveGeneration;
    private boolean acceptLiveResume;

    private volatile Hello hello;
    private volatile SnapshotRequest request;
    private final List<Envelope> afterFault = Collections.synchronizedList(new ArrayList<>());

    public SnapshotScript(SnapshotFixture fixture) {
        this.fixture = fixture;
        this.announcedSnapshot = fixture.info();
    }

    public SnapshotScript heartbeatIntervalMs(int ms) { this.heartbeatIntervalMs = ms; return this; }
    /** Answer Hello with this error instead of HelloAck. */
    public SnapshotScript rejectHello(ErrorReply error) { this.handshakeError = error; return this; }
    /** Send SnapshotQueued and this many heartbeats before SnapshotBegin. */
    public SnapshotScript queued(int heartbeats) { this.queuedHeartbeats = heartbeats; return this; }
    /** Inject a fault when chunk {@code seq} is due (CORRUPT_CRC sends it corrupted, the others stop before it). */
    public SnapshotScript faultAt(int seq, Fault fault) { this.faultSeq = seq; this.fault = fault; return this; }
    /** Send every chunk, then close without SnapshotEnd. */
    public SnapshotScript dropBeforeEnd() { this.dropBeforeEnd = true; return this; }
    /** Announce a different current snapshot in HelloAck (C-5), still serving the fixture. */
    public SnapshotScript announce(SnapshotInfo info) { this.announcedSnapshot = info; return this; }
    /** What to do after SnapshotEnd; by default read until the client closes. */
    public SnapshotScript afterEnd(FakeServer.ConnectionScript script) { this.afterEnd = script; return this; }

    /** Accept live-update subscriptions and offer this update generation in HelloAck. */
    public SnapshotScript live(ByteString generation) { this.liveGeneration = generation; return this; }
    /** Answer a request carrying live_resume with UpdatesResumed (then run {@link #afterEnd}) instead of a snapshot. */
    public SnapshotScript acceptLiveResume() { this.acceptLiveResume = true; return this; }

    public Hello hello() { return hello; }
    public SnapshotRequest request() { return request; }
    /** Frames the client sent after the fault (the INTEGRITY_FAILURE report, for instance). */
    public List<Envelope> afterFault() { return afterFault; }

    @Override public void run(FakeServer.Session s) throws Exception {
        Envelope first = s.read();
        assertEquals("first frame must be Hello", Envelope.BodyCase.HELLO, first.getBodyCase());
        hello = first.getHello();
        assertEquals(Framing.PROTOCOL_MAJOR, hello.getProtocolMajor());
        assertTrue(hello.getSchemaMajorsList().contains(Framing.SCHEMA_MAJOR));
        if (handshakeError != null) {
            s.send(Envelope.newBuilder().setError(handshakeError).build());
            return;
        }
        Codec codec = hello.getAcceptedCodecsList().contains(fixture.codec()) ? fixture.codec() : Codec.CODEC_NONE;
        assertEquals("client must accept the fixture's codec", fixture.codec(), codec);
        s.send(Envelope.newBuilder().setHelloAck(HelloAck.newBuilder().setProtocolMajor(Framing.PROTOCOL_MAJOR).setProtocolMinor(Framing.PROTOCOL_MINOR)
                .setSchemaMajor(Framing.SCHEMA_MAJOR).setSchemaMinor(Framing.SCHEMA_MINOR).setCodec(codec).setMaxFrameBytes(Framing.DEFAULT_MAX_FRAME_BYTES)
                .setHeartbeatIntervalMs(heartbeatIntervalMs).setServerId("fake").setServerVersion("test").setSessionId("s1")
                .setCurrentSnapshot(announcedSnapshot).setLiveUpdates(liveGeneration != null && hello.getLiveUpdates())
                .setUpdateGeneration(liveGeneration != null && hello.getLiveUpdates() ? liveGeneration : ByteString.EMPTY)).build());

        Envelope second;
        try {
            second = s.read();
        } catch (EOFException e) {
            return;   // the client declined this server after the handshake (data version check)
        }
        assertEquals("second frame must be SnapshotRequest", Envelope.BodyCase.SNAPSHOT_REQUEST, second.getBodyCase());
        request = second.getSnapshotRequest();
        assertTrue("request_id must be >= 1", request.getRequestId() >= 1);
        long id = request.getRequestId();
        if (acceptLiveResume && request.hasLiveResume()) {
            s.send(Envelope.newBuilder().setUpdatesResumed(UpdatesResumed.newBuilder().setRequestId(id)
                    .setDataVersion(request.getLiveResume().getDataVersion()).setCurrentSnapshot(announcedSnapshot)).build());
            afterEnd.run(s);
            return;
        }

        if (request.getIfSnapshotId().equals(fixture.info().getSnapshotId())) {
            s.send(Envelope.newBuilder().setSnapshotBegin(SnapshotBegin.newBuilder().setRequestId(id).setSnapshot(fixture.info()).setNotModified(true)).build());
            s.send(Envelope.newBuilder().setSnapshotEnd(fixture.end(id, List.of()).toBuilder().setChunksSent(0).setRecordsSent(0)).build());
            return;
        }
        List<Integer> types = request.getEntityTypesList().isEmpty() ? fixture.entityTypes()
                : fixture.entityTypes().stream().filter(request.getEntityTypesList()::contains).toList();
        List<SnapshotChunk> chunks = fixture.chunksFor(types);
        int fromSeq = chunks.isEmpty() ? 0 : chunks.get(0).getSeq();
        if (!request.getResumeSnapshotId().isEmpty()) {
            if (!Arrays.equals(request.getResumeSnapshotId().toByteArray(), fixture.snapshotId())) {
                s.send(Envelope.newBuilder().setError(ErrorReply.newBuilder().setRequestId(id).setCode(ErrorCode.ERROR_CODE_SNAPSHOT_NOT_AVAILABLE)
                        .setMessage("not this snapshot").setFatal(true)).build());
                return;
            }
            fromSeq = request.getResumeFromSeq();
        }
        for (int i = 0; i < queuedHeartbeats; i++) {
            if (i == 0) s.send(Envelope.newBuilder().setSnapshotQueued(SnapshotQueued.newBuilder().setRequestId(id).setQueuePosition(1).setMaxWaitMs(60_000)).build());
            Thread.sleep(20);
            s.send(Envelope.newBuilder().setHeartbeat(Heartbeat.newBuilder().setSentAtEpochMs(System.currentTimeMillis())).build());
        }
        s.send(Envelope.newBuilder().setSnapshotBegin(SnapshotBegin.newBuilder().setRequestId(id).setSnapshot(fixture.info()).addAllEntityTypes(types).setFirstSeq(fromSeq)).build());
        try {
            stream(s, id, types, chunks, fromSeq);
        } catch (SocketException e) {
            // the client closed the connection mid-stream (a failed check); a real server sees the same
        }
    }

    private void stream(FakeServer.Session s, long id, List<Integer> types, List<SnapshotChunk> chunks, int fromSeq) throws Exception {
        for (SnapshotChunk chunk : chunks) {
            if (chunk.getSeq() < fromSeq) continue;
            if (chunk.getSeq() == faultSeq) {
                switch (fault) {
                    case DROP_CONNECTION -> { return; }
                    case GO_AWAY -> {
                        s.send(Envelope.newBuilder().setGoAway(GoAway.newBuilder().setReason(ErrorCode.ERROR_CODE_SERVER_SHUTTING_DOWN).setMessage("bye").setReconnectAfterMs(10)).build());
                        return;
                    }
                    case CORRUPT_CRC -> {
                        byte[] payload = chunk.getPayload().toByteArray();
                        payload[payload.length / 2] ^= 0x7f;
                        s.send(Envelope.newBuilder().setSnapshotChunk(chunk.toBuilder().setPayload(ByteString.copyFrom(payload))).build());
                        afterFault.addAll(s.drain());
                        return;
                    }
                }
            }
            s.send(Envelope.newBuilder().setSnapshotChunk(chunk).build());
        }
        if (dropBeforeEnd) return;
        s.send(Envelope.newBuilder().setSnapshotEnd(fixture.end(id, types)).build());
        afterEnd.run(s);
    }
}
