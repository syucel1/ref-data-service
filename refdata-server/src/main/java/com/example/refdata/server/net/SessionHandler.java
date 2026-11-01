package com.example.refdata.server.net;

import com.example.refdata.contract.Digests;
import com.example.refdata.contract.Framing;
import com.example.refdata.model.EntityKind;
import com.example.refdata.protocol.v1.Codec;
import com.example.refdata.protocol.v1.EntityManifest;
import com.example.refdata.protocol.v1.Envelope;
import com.example.refdata.protocol.v1.ErrorCode;
import com.example.refdata.protocol.v1.ErrorReply;
import com.example.refdata.protocol.v1.GoAway;
import com.example.refdata.protocol.v1.Heartbeat;
import com.example.refdata.protocol.v1.Hello;
import com.example.refdata.protocol.v1.HelloAck;
import com.example.refdata.protocol.v1.SnapshotBegin;
import com.example.refdata.protocol.v1.SnapshotEnd;
import com.example.refdata.protocol.v1.SnapshotQueued;
import com.example.refdata.protocol.v1.SnapshotRequest;
import com.example.refdata.server.ServerConfig;
import com.example.refdata.server.health.ServerMetrics;
import com.example.refdata.server.snapshot.EncodedChunk;
import com.example.refdata.server.snapshot.Snapshot;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.TooLongFrameException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * One session = one connection: the state machine {@code AWAIT_HELLO -> READY -> (QUEUED ->) STREAMING -> READY}
 * of DESIGN.md 9.4 / 13.2 and the implementation notes. Everything runs on the channel's event loop; the only
 * cross-thread entry is {@link #admitted()} from the {@link AdmissionController}, which hops onto the loop.
 * Plain Netty handler, so it is testable with an {@code EmbeddedChannel}: inbound {@link Envelope}s in, outbound
 * {@link Envelope}s (control messages) and {@link ByteBuf}s (pre-encoded chunk frames) out.
 */
public final class SessionHandler extends SimpleChannelInboundHandler<Envelope> implements AdmissionController.Waiter {
    private static final Logger log = LoggerFactory.getLogger(SessionHandler.class);
    private static final Logger sessionLog = LoggerFactory.getLogger("com.example.refdata.server.sessions");
    private static final AtomicLong SESSION_IDS = new AtomicLong();
    /** How long a fatal error frame may wait for a slow reader before the connection is closed anyway. */
    private static final long CLOSE_GRACE_MS = 1_000;
    /** Retry hint after a queue wait ran out: a slot may free at any moment, so the client should come back soon. */
    static final int RETRY_AFTER_WAIT_EXPIRED_MS = 3_000;

    /** What the server tells a session about its own state, and how a session reports a corrupt stored frame. */
    public interface TransferGate {
        /** False once the instance must not start new transfers (design S-16). */
        boolean acceptingTransfers();
        /** A client's INTEGRITY_FAILURE report was confirmed against the stored frame. */
        void storedFrameCorrupt(int seq, Codec codec);

        /** A gate that always admits and only logs (for tests). */
        TransferGate OPEN = new TransferGate() {
            @Override public boolean acceptingTransfers() { return true; }
            @Override public void storedFrameCorrupt(int seq, Codec codec) { }
        };
    }

    /** Session states (design 9.4). */
    public enum State { AWAIT_HELLO, READY, QUEUED, STREAMING, CLOSED }

    private final ServerConfig config;
    private final Snapshot snapshot;
    private final AdmissionController admission;
    private final TransferGate gate;
    private final ServerMetrics metrics;
    private final String serverId;
    private final String serverVersion;
    private final String sessionId;

    private ChannelHandlerContext ctx;
    private volatile State state = State.AWAIT_HELLO;   // read by the shutdown thread through streaming()
    private long openedNanos;
    private String clientName = "", clientInstance = "", libraryVersion = "";
    private int protocolMajor, protocolMinor, schemaMajor;
    private Codec codec = Codec.CODEC_NONE;
    private boolean keepAlive;
    private long bytesSent;
    private long bytesAcknowledged;
    private long bytesAtLastStallCheck;
    private String outcome = "open";
    private boolean goingAway;
    private int goAwayReconnectMs;

    // current transfer
    private long requestId;
    private List<EntityKind> kinds = List.of();
    private int[] rangeFirst = new int[0], rangeEnd = new int[0];   // requested seq ranges in stream order, [first, end)
    private int rangeIndex;
    private int nextSeq;
    private int firstSeq;
    private boolean resumed;
    private long transferStartNanos;

    private ScheduledFuture<?> handshakeTimer, idleTimer, heartbeatTimer, queueTimer, durationTimer, stallTimer;

    public SessionHandler(ServerConfig config, Snapshot snapshot, AdmissionController admission, TransferGate gate, ServerMetrics metrics,
                          String serverId, String serverVersion) {
        this.config = config;
        this.snapshot = snapshot;
        this.admission = admission;
        this.gate = gate;
        this.metrics = metrics;
        this.serverId = serverId;
        this.serverVersion = serverVersion;
        this.sessionId = serverId + "-" + Long.toHexString(SESSION_IDS.incrementAndGet());
    }

    public State state() { return state; }
    public String sessionId() { return sessionId; }
    public long bytesSent() { return bytesSent; }

    // ------------------------------------------------------------------ lifecycle

    @Override public void handlerAdded(ChannelHandlerContext ctx) {
        this.ctx = ctx;
    }

    @Override public void channelActive(ChannelHandlerContext ctx) {
        openedNanos = System.nanoTime();
        metrics.sessionOpened();
        handshakeTimer = schedule(config.handshakeTimeout.toMillis(), () -> {
            if (state == State.AWAIT_HELLO) fail(ErrorCode.ERROR_CODE_HANDSHAKE_TIMEOUT, "no Hello within " + config.handshakeTimeout.toMillis() + " ms");
        });
        ctx.fireChannelActive();
    }

    @Override public void channelInactive(ChannelHandlerContext ctx) {
        if (openedNanos == 0) { ctx.fireChannelInactive(); return; }   // never active (rejected by the connection limiter)
        State last = state;
        if (last == State.QUEUED) {
            if (admission.cancel(this)) metrics.dequeued();             // else admitted() is on its way and will release the slot
        } else if (last == State.STREAMING) {
            admission.release();
            metrics.transferFailed(outcome.equals("open") ? "disconnect" : outcome);
        }
        if (outcome.equals("open")) outcome = last == State.STREAMING ? "disconnect-mid-stream" : "closed-by-client";
        state = State.CLOSED;
        cancelTimers();
        metrics.sessionClosed(clientName, outcome);
        long ms = (System.nanoTime() - openedNanos) / 1_000_000;
        sessionLog.info("session {} closed: client={} instance={} library={} protocol={}.{} schema={} codec={} keepAlive={} snapshot={} bytes={} durationMs={} outcome={} lastState={} remote={}",
                sessionId, clientName, clientInstance, libraryVersion, protocolMajor, protocolMinor, schemaMajor, codec, keepAlive,
                snapshot.snapshotIdHex(), bytesSent, ms, outcome, last, ctx.channel().remoteAddress());
        ctx.fireChannelInactive();
    }

    @Override public void channelWritabilityChanged(ChannelHandlerContext ctx) {
        if (state == State.STREAMING && ctx.channel().isWritable()) pump();
        ctx.fireChannelWritabilityChanged();
    }

    @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        Throwable t = cause instanceof DecoderException && cause.getCause() != null ? cause.getCause() : cause;
        if (state == State.CLOSED) return;
        if (t instanceof TooLongFrameException) {
            fail(ErrorCode.ERROR_CODE_FRAME_TOO_LARGE, "inbound frame larger than " + Framing.SERVER_INBOUND_MAX_FRAME_BYTES + " bytes");
        } else if (t instanceof CorruptedFrameException) {
            fail(ErrorCode.ERROR_CODE_PROTOCOL_VIOLATION, t.getMessage());
        } else if (t instanceof IOException) {
            log.debug("session {} I/O error: {}", sessionId, t.toString());
            outcome = "io-error";
            ctx.close();
        } else {
            log.error("session {} unexpected error", sessionId, t);
            metrics.sessionError();
            fail(ErrorCode.ERROR_CODE_INTERNAL_ERROR, "internal error");
        }
    }

    // ------------------------------------------------------------------ inbound messages

    @Override protected void channelRead0(ChannelHandlerContext ctx, Envelope msg) {
        if (state == State.CLOSED) return;
        if (msg.getBodyCase() == Envelope.BodyCase.ERROR) {
            onClientError(msg.getError());
            return;
        }
        if (msg.getBodyCase() == Envelope.BodyCase.BODY_NOT_SET) {
            if (state == State.AWAIT_HELLO) fail(ErrorCode.ERROR_CODE_PROTOCOL_VIOLATION, "expected Hello as the first message");
            else reply(ErrorCode.ERROR_CODE_UNSUPPORTED_MESSAGE, 0, "envelope body not understood by this server", false);
            return;
        }
        touch();
        switch (state) {
            case AWAIT_HELLO -> {
                if (msg.getBodyCase() != Envelope.BodyCase.HELLO) fail(ErrorCode.ERROR_CODE_PROTOCOL_VIOLATION, "expected Hello as the first message, got " + msg.getBodyCase());
                else onHello(msg.getHello());
            }
            case READY, QUEUED, STREAMING -> {
                switch (msg.getBodyCase()) {
                    case HELLO -> fail(ErrorCode.ERROR_CODE_PROTOCOL_VIOLATION, "second Hello");
                    case SNAPSHOT_REQUEST -> {
                        if (state == State.READY) onRequest(msg.getSnapshotRequest());
                        else reply(ErrorCode.ERROR_CODE_INVALID_REQUEST, msg.getSnapshotRequest().getRequestId(), "a snapshot request is already " + state.name().toLowerCase(), false);
                    }
                    case HEARTBEAT -> { /* liveness only; touch() already noted it */ }
                    default -> fail(ErrorCode.ERROR_CODE_PROTOCOL_VIOLATION, "unexpected " + msg.getBodyCase() + " from a client");
                }
            }
            case CLOSED -> { }
        }
    }

    private void onHello(Hello hello) {
        clientName = hello.getClientName();
        clientInstance = hello.getClientInstance();
        libraryVersion = hello.getLibraryVersion();
        protocolMajor = hello.getProtocolMajor();
        protocolMinor = hello.getProtocolMinor();
        keepAlive = hello.getKeepAlive();
        if (hello.getProtocolMajor() != Framing.PROTOCOL_MAJOR) {
            fail(ErrorCode.ERROR_CODE_UNSUPPORTED_PROTOCOL_VERSION, "protocol major " + hello.getProtocolMajor() + " not supported");
            return;
        }
        if (!hello.getSchemaMajorsList().contains(Framing.SCHEMA_MAJOR)) {
            fail(ErrorCode.ERROR_CODE_UNSUPPORTED_SCHEMA_VERSION, "no common schema major in " + hello.getSchemaMajorsList());
            return;
        }
        schemaMajor = Framing.SCHEMA_MAJOR;
        codec = Codec.CODEC_NONE;
        for (Codec c : hello.getAcceptedCodecsList()) {            // first codec in the client's order that the server has, NONE included
            if (c == Codec.CODEC_NONE || c == Codec.CODEC_UNSPECIFIED) break;
            if (snapshot.supports(c)) { codec = c; break; }
        }
        int clientMax = hello.getMaxFrameBytes() == 0 ? Framing.DEFAULT_MAX_FRAME_BYTES : hello.getMaxFrameBytes();
        int largest = snapshot.largestFrameBytes(codec);
        if (clientMax < largest) {
            fail(ErrorCode.ERROR_CODE_FRAME_TOO_LARGE, "client accepts " + clientMax + " byte frames, the snapshot needs " + largest);
            return;
        }
        cancel(handshakeTimer);
        HelloAck ack = HelloAck.newBuilder().setProtocolMajor(Framing.PROTOCOL_MAJOR).setProtocolMinor(Framing.PROTOCOL_MINOR)
                .setSchemaMajor(Framing.SCHEMA_MAJOR).setSchemaMinor(Framing.SCHEMA_MINOR).setCodec(codec)
                .setMaxFrameBytes(Math.min(config.frameMaxBytes, clientMax)).setHeartbeatIntervalMs((int) config.heartbeatInterval.toMillis())
                .setServerId(serverId).setServerVersion(serverVersion).setSessionId(sessionId).setCurrentSnapshot(snapshot.info()).build();
        send(Envelope.newBuilder().setHelloAck(ack).build());
        enterReady();
        if (keepAlive) heartbeatTimer = schedulePeriodic(config.heartbeatInterval.toMillis(), this::heartbeatIfIdle);
        log.debug("session {} handshake done: client={} instance={} codec={} keepAlive={}", sessionId, clientName, clientInstance, codec, keepAlive);
    }

    private void onRequest(SnapshotRequest req) {
        long id = req.getRequestId();
        if (id == 0) { reply(ErrorCode.ERROR_CODE_INVALID_REQUEST, 0, "request_id must be >= 1", false); return; }
        List<EntityKind> requested = resolveKinds(req.getEntityTypesList());
        if (!req.getIfSnapshotId().isEmpty() && snapshot.hasId(req.getIfSnapshotId())) {
            metrics.notModified();
            send(Envelope.newBuilder().setSnapshotBegin(SnapshotBegin.newBuilder().setRequestId(id).setSnapshot(snapshot.info()).setNotModified(true)
                    .addAllEntityTypes(wireCodes(requested)).setFirstSeq(0)).build());
            send(Envelope.newBuilder().setSnapshotEnd(SnapshotEnd.newBuilder().setRequestId(id).setChunksSent(0).setRecordsSent(0)
                    .setContentSha256(snapshot.info().getContentSha256())).build());
            return;
        }
        int[] first = new int[requested.size()], end = new int[requested.size()];
        int startSeq = -1, startRange = 0;
        for (int i = 0; i < requested.size(); i++) {
            EntityManifest m = snapshot.manifest(requested.get(i));
            first[i] = m.getFirstSeq();
            end[i] = m.getFirstSeq() + m.getChunkCount();
            if (startSeq < 0 && m.getChunkCount() > 0) { startSeq = first[i]; startRange = i; }
        }
        boolean resume = !req.getResumeSnapshotId().isEmpty();
        if (resume) {
            if (!snapshot.hasId(req.getResumeSnapshotId())) {
                reply(ErrorCode.ERROR_CODE_SNAPSHOT_NOT_AVAILABLE, id, "snapshot " + Digests.hex(req.getResumeSnapshotId().toByteArray()) + " is not served here (current " + snapshot.snapshotIdHex() + ")", false);
                return;
            }
            int seq = req.getResumeFromSeq();
            int range = -1;
            for (int i = 0; i < requested.size(); i++) if (seq >= first[i] && seq < end[i]) { range = i; break; }
            int endOfLast = 0;
            for (int e : end) endOfLast = Math.max(endOfLast, e);
            if (range < 0 && seq == endOfLast && startSeq >= 0) {
                range = requested.size();                            // every chunk was received, only SnapshotEnd was lost: nothing to stream
            } else if (range < 0) {
                reply(ErrorCode.ERROR_CODE_INVALID_REQUEST, id, "resume_from_seq " + seq + " is outside the requested chunk ranges", false);
                return;
            }
            startSeq = seq;
            startRange = range;
        }
        if (startSeq < 0) { startRange = requested.size(); startSeq = 0; }   // nothing to send: every requested kind is empty
        requestId = id;
        kinds = requested;
        rangeFirst = first;
        rangeEnd = end;
        rangeIndex = startRange;
        nextSeq = startSeq < 0 ? 0 : startSeq;
        firstSeq = nextSeq;
        resumed = resume;
        if (!gate.acceptingTransfers()) {
            serverBusy(id, "this instance is not serving transfers (stored snapshot failed its integrity check)", (int) config.snapshotQueueMaxWait.toMillis());
            return;
        }
        AdmissionController.Decision d = admission.request(this);
        switch (d.outcome()) {
            case ADMITTED -> startStreaming();
            case QUEUED -> {
                state = State.QUEUED;
                cancel(idleTimer);
                metrics.queued();
                send(Envelope.newBuilder().setSnapshotQueued(SnapshotQueued.newBuilder().setRequestId(id).setQueuePosition(d.position())
                        .setMaxWaitMs((int) config.snapshotQueueMaxWait.toMillis())).build());
                if (heartbeatTimer == null) heartbeatTimer = schedulePeriodic(config.heartbeatInterval.toMillis(), this::heartbeatIfIdle);
                queueTimer = schedule(config.snapshotQueueMaxWait.toMillis(), this::queueWaitExpired);
            }
            case BUSY -> serverBusy(id, "transfer slots and queue are full", (int) config.snapshotQueueMaxWait.toMillis());
        }
    }

    private void onClientError(ErrorReply err) {
        if (err.getCode() == ErrorCode.ERROR_CODE_INTEGRITY_FAILURE) {
            int seq = err.getSeq();
            boolean intact = seq >= 0 && seq < snapshot.chunkCount() && snapshot.chunk(seq).verifyCrc(codec);
            metrics.integrityReport(!intact);
            if (intact) {
                log.warn("session {} (client {} {}) reported INTEGRITY_FAILURE for chunk {}: stored frame re-verified OK ({}); message: {}",
                        sessionId, clientName, clientInstance, seq, codec, err.getMessage());
            } else {
                log.error("session {} (client {} {}) reported INTEGRITY_FAILURE for chunk {} and the stored frame FAILS its CRC re-check ({}) - memory corruption or a buffer bug; message: {}",
                        sessionId, clientName, clientInstance, seq, codec, err.getMessage());
                gate.storedFrameCorrupt(seq, codec);
            }
            outcome = "integrity-failure-reported";
        } else {
            log.warn("session {} sent ErrorReply {} ({}), fatal={}", sessionId, err.getCode(), err.getMessage(), err.getFatal());
        }
    }

    // ------------------------------------------------------------------ streaming

    /** Called by the admission controller, on any thread, when a queued request gets a slot. */
    @Override public void admitted() {
        if (ctx.executor().inEventLoop()) onAdmitted(); else ctx.executor().execute(this::onAdmitted);
    }

    private void onAdmitted() {
        if (state != State.QUEUED) { admission.release(); return; }   // closed, or the wait expired first
        cancel(queueTimer);
        stopQueueHeartbeats();
        metrics.dequeued();
        startStreaming();
    }

    private void queueWaitExpired() {
        if (state != State.QUEUED) return;
        if (!admission.cancel(this)) return;                          // admitted concurrently: onAdmitted() is on its way
        stopQueueHeartbeats();
        metrics.dequeued();
        enterReady();
        serverBusy(requestId, "no transfer slot within " + config.snapshotQueueMaxWait.toMillis() + " ms", RETRY_AFTER_WAIT_EXPIRED_MS);
    }

    /** Heartbeats were only for the wait unless the connection is kept alive. */
    private void stopQueueHeartbeats() {
        if (!keepAlive) { cancel(heartbeatTimer); heartbeatTimer = null; }
    }

    private void startStreaming() {
        if (!ctx.channel().isActive()) { admission.release(); return; }
        state = State.STREAMING;
        cancel(idleTimer);
        transferStartNanos = System.nanoTime();
        metrics.transferStarted(resumed);
        send(Envelope.newBuilder().setSnapshotBegin(SnapshotBegin.newBuilder().setRequestId(requestId).setSnapshot(snapshot.info())
                .setNotModified(false).addAllEntityTypes(wireCodes(kinds)).setFirstSeq(firstSeq)).build());
        durationTimer = schedule(config.snapshotMaxDuration.toMillis(), () -> {
            if (state == State.STREAMING) { outcome = "max-duration"; fail(ErrorCode.ERROR_CODE_SLOW_CONSUMER, "transfer exceeded " + config.snapshotMaxDuration.toMillis() + " ms"); }
        });
        bytesAtLastStallCheck = bytesAcknowledged;
        stallTimer = schedulePeriodic(config.stallTimeout.toMillis(), this::stallCheck);
        pump();
    }

    /** Writes chunk frames while the channel is writable; finishes the transfer when the last range is done. */
    private void pump() {
        while (state == State.STREAMING && ctx.channel().isWritable()) {
            while (rangeIndex < rangeFirst.length && nextSeq >= rangeEnd[rangeIndex]) {
                rangeIndex++;
                if (rangeIndex < rangeFirst.length) nextSeq = rangeFirst[rangeIndex];
            }
            if (rangeIndex >= rangeFirst.length) {
                ctx.flush();
                finishTransfer();
                return;
            }
            EncodedChunk chunk = snapshot.chunk(nextSeq++);
            int len = chunk.frameBytes(codec);
            bytesSent += len;
            ctx.write(chunk.frameBuffer(codec)).addListener(f -> { if (f.isSuccess()) { bytesAcknowledged += len; metrics.chunkSent(len); } });
        }
        ctx.flush();
    }

    private void finishTransfer() {
        long chunks = 0, records = 0;
        for (EntityKind k : kinds) { EntityManifest m = snapshot.manifest(k); chunks += m.getChunkCount(); records += m.getRecordCount(); }
        send(Envelope.newBuilder().setSnapshotEnd(SnapshotEnd.newBuilder().setRequestId(requestId).setChunksSent((int) chunks).setRecordsSent(records)
                .setContentSha256(snapshot.info().getContentSha256())).build());
        cancel(durationTimer);
        cancel(stallTimer);
        admission.release();
        metrics.transferCompleted();
        log.info("session {} transfer {} done: client={} kinds={} firstSeq={} chunks={} records={} codec={} ms={}", sessionId, requestId, clientName,
                kinds, firstSeq, chunks, records, codec, (System.nanoTime() - transferStartNanos) / 1_000_000);
        kinds = List.of();
        if (goingAway) { sendGoAwayAndClose(); return; }
        enterReady();
    }

    private void stallCheck() {
        if (state != State.STREAMING) return;
        if (bytesAcknowledged == bytesAtLastStallCheck) {
            outcome = "stall";
            fail(ErrorCode.ERROR_CODE_SLOW_CONSUMER, "no write progress for " + config.stallTimeout.toMillis() + " ms");
            return;
        }
        bytesAtLastStallCheck = bytesAcknowledged;
    }

    // ------------------------------------------------------------------ shutdown

    /** Planned shutdown (P-17): tells the client to reconnect elsewhere; a running transfer is allowed to finish. */
    public void goAway(int reconnectAfterMs) {
        if (state == State.CLOSED || goingAway) return;
        goingAway = true;
        goAwayReconnectMs = reconnectAfterMs;
        if (state != State.STREAMING) sendGoAwayAndClose();       // a running transfer finishes first (within the drain), then gets its GoAway
    }

    private void sendGoAwayAndClose() {
        outcome = "go-away";
        ctx.writeAndFlush(Envelope.newBuilder().setGoAway(GoAway.newBuilder().setReason(ErrorCode.ERROR_CODE_SERVER_SHUTTING_DOWN)
                .setMessage("server shutting down").setReconnectAfterMs(goAwayReconnectMs)).build()).addListener(ChannelFutureListener.CLOSE);
    }

    /** True while a transfer is in progress (what the drain phase of a shutdown waits for). */
    public boolean streaming() { return state == State.STREAMING; }

    // ------------------------------------------------------------------ helpers

    private void enterReady() {
        state = State.READY;
        resetIdleTimer();
    }

    /** Any inbound frame counts as liveness. */
    private void touch() {
        if (state == State.READY) resetIdleTimer();
    }

    private void resetIdleTimer() {
        cancel(idleTimer);
        if (keepAlive) {
            long limit = config.heartbeatInterval.toMillis() * Framing.HEARTBEAT_MISSES;
            idleTimer = schedule(limit, () -> { if (state == State.READY) { outcome = "heartbeat-timeout"; ctx.close(); } });
        } else {
            idleTimer = schedule(config.sessionIdleTimeout.toMillis(), () -> {
                if (state == State.READY) fail(ErrorCode.ERROR_CODE_IDLE_TIMEOUT, "no request within " + config.sessionIdleTimeout.toMillis() + " ms");
            });
        }
    }

    private void heartbeatIfIdle() {
        if (state == State.READY || state == State.QUEUED) {
            send(Envelope.newBuilder().setHeartbeat(Heartbeat.newBuilder().setSentAtEpochMs(System.currentTimeMillis())).build());
        }
    }

    private void serverBusy(long id, String why, int retryAfterMs) {
        metrics.serverBusy();
        send(Envelope.newBuilder().setError(ErrorReply.newBuilder().setRequestId(id).setCode(ErrorCode.ERROR_CODE_SERVER_BUSY).setMessage(why)
                .setFatal(false).setRetryAfterMs(retryAfterMs)).build());
    }

    private void reply(ErrorCode code, long id, String message, boolean fatal) {
        ErrorReply.Builder err = ErrorReply.newBuilder().setRequestId(id).setCode(code).setMessage(message).setFatal(fatal)
                .setSupportedProtocolMin(Framing.PROTOCOL_MAJOR).setSupportedProtocolMax(Framing.PROTOCOL_MAJOR).addSupportedSchemaMajors(Framing.SCHEMA_MAJOR);
        send(Envelope.newBuilder().setError(err).build());
    }

    /** Sends a fatal error and closes once it is flushed. */
    private void fail(ErrorCode code, String message) {
        if (state == State.CLOSED) return;
        if (outcome.equals("open")) outcome = code.name().substring("ERROR_CODE_".length()).toLowerCase();
        log.info("session {} closing with {}: {}", sessionId, code, message);
        cancelTimers();
        ErrorReply.Builder err = ErrorReply.newBuilder().setCode(code).setMessage(message).setFatal(true)
                .setSupportedProtocolMin(Framing.PROTOCOL_MAJOR).setSupportedProtocolMax(Framing.PROTOCOL_MAJOR).addSupportedSchemaMajors(Framing.SCHEMA_MAJOR);
        State previous = state;
        state = State.CLOSED;
        if (previous == State.QUEUED) { if (admission.cancel(this)) metrics.dequeued(); }   // else admitted() will release the slot
        else if (previous == State.STREAMING) { admission.release(); metrics.transferFailed(outcome); }
        ChannelFuture f = ctx.writeAndFlush(Envelope.newBuilder().setError(err).build());
        f.addListener(ChannelFutureListener.CLOSE);
        // a client that is not reading would keep the error frame (and the connection) pending forever
        schedule(CLOSE_GRACE_MS, () -> { if (!f.isDone()) ctx.close(); });
    }

    private ChannelFuture send(Envelope env) {
        metrics.controlBytesSent(Framing.LENGTH_PREFIX_BYTES + env.getSerializedSize());
        return ctx.writeAndFlush(env);
    }

    /** Requested kinds in stream order; empty request = all; unknown numbers ignored; duplicates collapsed. */
    private static List<EntityKind> resolveKinds(List<Integer> wire) {
        if (wire.isEmpty()) return List.of(EntityKind.values());
        EnumSet<EntityKind> set = EnumSet.noneOf(EntityKind.class);
        for (int code : wire) { EntityKind k = EntityKind.fromWire(code); if (k != null) set.add(k); }
        return new ArrayList<>(set);
    }

    private static List<Integer> wireCodes(List<EntityKind> kinds) {
        List<Integer> out = new ArrayList<>(kinds.size());
        for (EntityKind k : kinds) out.add(k.wireCode());
        return out;
    }

    private ScheduledFuture<?> schedule(long delayMs, Runnable task) {
        return ctx.executor().schedule(task, delayMs, TimeUnit.MILLISECONDS);
    }

    private ScheduledFuture<?> schedulePeriodic(long periodMs, Runnable task) {
        return ctx.executor().scheduleAtFixedRate(task, periodMs, periodMs, TimeUnit.MILLISECONDS);
    }

    private static void cancel(ScheduledFuture<?> f) {
        if (f != null) f.cancel(false);
    }

    private void cancelTimers() {
        cancel(handshakeTimer); cancel(idleTimer); cancel(heartbeatTimer); cancel(queueTimer); cancel(durationTimer); cancel(stallTimer);
    }
}
