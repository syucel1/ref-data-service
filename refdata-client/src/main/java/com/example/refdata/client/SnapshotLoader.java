package com.example.refdata.client;

import com.example.refdata.contract.Digests;
import com.example.refdata.contract.Framing;
import com.example.refdata.model.ChangeBatch;
import com.example.refdata.model.RefDataStoreBuilder;
import com.example.refdata.model.validation.ValidationException;
import com.example.refdata.protocol.v1.Envelope;
import com.example.refdata.protocol.v1.ErrorReply;
import com.example.refdata.protocol.v1.GoAway;
import com.example.refdata.protocol.v1.Heartbeat;
import com.example.refdata.protocol.v1.Hello;
import com.example.refdata.protocol.v1.HelloAck;
import com.example.refdata.protocol.v1.LiveResume;
import com.example.refdata.protocol.v1.SnapshotBegin;
import com.example.refdata.protocol.v1.SnapshotChunk;
import com.example.refdata.protocol.v1.SnapshotEnd;
import com.example.refdata.protocol.v1.SnapshotInfo;
import com.example.refdata.protocol.v1.SnapshotQueued;
import com.example.refdata.protocol.v1.SnapshotRequest;
import com.example.refdata.protocol.v1.UpdatesResumed;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;

/**
 * One load: cycles through the hosts with back-off, performs the handshake and the snapshot conversation, keeps the
 * staging area across connections so a dropped transfer is resumed, and applies the recovery rules of design
 * section 13.3 (C-1 to C-5, C-10, C-11, C-14). Single use, single thread.
 */
final class SnapshotLoader {
    private static final Logger log = LoggerFactory.getLogger(SnapshotLoader.class);
    static final String LIBRARY_VERSION = "refdata-client-java 0.1.0";
    /** Full failures (hash, totals, repeated chunk failure) before the load gives up (C-4). */
    static final int MAX_FULL_FAILURES = 3;
    /** Rejections of the same chunk before that counts as a full failure (C-3). */
    static final int MAX_FAILURES_PER_CHUNK = 2;

    private final ClientSettings settings;
    private final LoadedSnapshot knownSnapshot;
    private final StatsCollector stats = new StatsCollector();
    private final Backoff backoff;
    private final long deadlineNanos;

    private SnapshotStaging staging;
    private long nextRequestId = 1;
    private long currentRequestId;
    private int fullFailures;
    private int hostIndex;
    private int versionRejections;
    private KeepAliveSession keepAliveSession;
    private volatile boolean cancelled;
    private volatile ServerConnection active;

    /** Live-update subscription: set by {@link #onLiveUpdates}. */
    private boolean liveUpdates;
    private Consumer<LoadedSnapshot> snapshotListener = s -> { };
    private Consumer<ChangeBatch> updateListener = b -> { };
    /** Live resume: the position to continue from and the data held at it; null = full snapshot. */
    private LiveResume livePosition;
    private LoadedSnapshot liveHeld;
    private boolean resumedLive;
    private ByteString updateGeneration = ByteString.EMPTY;

    SnapshotLoader(ClientSettings settings, LoadedSnapshot knownSnapshot) {
        this.settings = settings;
        this.knownSnapshot = knownSnapshot;
        this.backoff = new Backoff(settings.backoffInitial(), settings.backoffMax(), new Random());
        this.deadlineNanos = System.nanoTime() + settings.startupTimeout().toNanos();
    }

    /**
     * Subscribes to live updates: the loaded snapshot is handed to {@code snapshotListener} before the kept-alive
     * connection starts delivering batches to {@code updateListener} (on its reader thread).
     */
    SnapshotLoader onLiveUpdates(Consumer<LoadedSnapshot> snapshotListener, Consumer<ChangeBatch> updateListener) {
        this.liveUpdates = true;
        this.snapshotListener = snapshotListener;
        this.updateListener = updateListener;
        return this;
    }

    /**
     * Asks to continue a live subscription from {@code held}'s data version instead of loading a snapshot. A server of
     * the same update generation that still retains the updates after that version answers UpdatesResumed and
     * {@link #load()} returns {@code held} unchanged (the updates then arrive on the kept-alive connection); any other
     * server sends a full snapshot as usual.
     */
    SnapshotLoader resumeLive(ByteString generation, LoadedSnapshot held) {
        if (!liveUpdates) throw new IllegalStateException("resumeLive requires onLiveUpdates");
        this.livePosition = LiveResume.newBuilder().setGeneration(generation).setDataVersion(held.store().dataVersion()).build();
        this.liveHeld = held;
        return this;
    }

    /** Whether {@link #load()} continued a live subscription without a snapshot. */
    boolean resumedLive() { return resumedLive; }

    /** The update generation of the server the load succeeded on (empty when it offers no live resume). */
    ByteString updateGeneration() { return updateGeneration; }

    /** Aborts the load from another thread: closes the active connection; {@link #load()} then fails. */
    void cancel() {
        cancelled = true;
        ServerConnection c = active;
        if (c != null) c.close();
    }

    /** The kept-alive connection, when the settings ask for one and the load succeeded. */
    KeepAliveSession keepAliveSession() { return keepAliveSession; }

    LoadedSnapshot load() {
        Exception lastFailure = null;
        Duration delay = null;
        boolean first = true;
        while (true) {
            if (cancelled) throw new RefDataClientException("load cancelled");
            if (!first) pause(delay == null ? backoff.next() : delay, lastFailure);
            first = false;
            delay = null;
            Endpoint endpoint = settings.endpoints().get(Math.floorMod(hostIndex, settings.endpoints().size()));
            MDC.put("rds.endpoint", endpoint.toString());
            MDC.remove("rds.request");
            MDC.remove("rds.snapshot");
            MDC.remove("rds.session");
            stats.connections++;
            stats.phase(LoadPhase.CONNECTING);
            ServerConnection connection = null;
            boolean keepOpen = false;
            try {
                log.info("connecting to {} (attempt {})", endpoint, stats.connections);
                connection = ServerConnection.open(endpoint, settings.connectTimeout(), settings.readTimeout(), settings.maxFrameBytes());
                active = connection;
                if (cancelled) { connection.close(); throw new RefDataClientException("load cancelled"); }
                stats.phase(LoadPhase.HANDSHAKE);
                HelloAck ack = connection.handshake(hello());
                if (liveUpdates && !ack.getLiveUpdates()) throw new UnsupportedVersionException("server does not support live updates", null);
                MDC.put("rds.session", ack.getSessionId());
                MDC.put("rds.snapshot", Digests.hex(ack.getCurrentSnapshot().getSnapshotId().toByteArray()));
                if (!dataVersionAcceptable(ack.getCurrentSnapshot())) {
                    versionRejections++;
                    if (versionRejections >= settings.endpoints().size()) {
                        throw new DataVersionException("no host serves an acceptable data version (expected "
                                + settings.expectedDataVersion() + ", minimum " + settings.minDataVersion() + "; last seen "
                                + ack.getCurrentSnapshot().getDataVersion() + " at " + endpoint + ")");
                    }
                    hostIndex++;
                    continue;
                }
                versionRejections = 0;
                LoadedSnapshot result = transfer(connection, ack);
                keepOpen = keepAliveSession != null;
                return result;
            } catch (ServerBusyException e) {
                log.warn("{} is busy: {}", endpoint, e.getMessage());
                lastFailure = e;
                delay = e.retryAfter().isZero() ? null : e.retryAfter();
                hostIndex++;
            } catch (Reconnect e) {
                log.warn("reconnecting after {}: {}", endpoint, e.getMessage());
                lastFailure = e;
                delay = e.delay();
                if (e.nextHost()) hostIndex++;
            } catch (ChunkIntegrityFailure e) {
                stats.integrityFailures++;
                log.warn("integrity failure from {}: {}", endpoint, e.getMessage());
                connection.reportIntegrityFailure(currentRequestId, e.seq(), e.getMessage());
                int failures = staging.recordFailure();
                if (failures >= MAX_FAILURES_PER_CHUNK) fullFailure("chunk " + e.seq() + " rejected " + failures + " times", e);
                lastFailure = e;
                hostIndex++;
            } catch (SnapshotCorruption e) {
                fullFailure(e.getMessage(), e);
                lastFailure = e;
                hostIndex++;
            } catch (ProtocolException | IOException e) {
                log.warn("connection to {} failed in phase {}: {}", endpoint, stats.phase(), e.toString());
                lastFailure = e;
                hostIndex++;
            } catch (RefDataClientException e) {
                throw e;                                   // fatal: version, data version, integrity, validation
            } catch (RuntimeException e) {
                log.warn("unexpected error on the connection to {} in phase {}; treating it as a connection failure", endpoint, stats.phase(), e);
                lastFailure = e;
                hostIndex++;
            } finally {
                if (connection != null && !keepOpen) connection.close();
            }
        }
    }

    // ------------------------------------------------------------------------------------------------ one connection

    private Hello hello() {
        return Hello.newBuilder().setProtocolMajor(Framing.PROTOCOL_MAJOR).setProtocolMinor(Framing.PROTOCOL_MINOR)
                .addSchemaMajors(Framing.SCHEMA_MAJOR).setMaxFrameBytes(settings.maxFrameBytes()).setClientName(settings.clientName())
                .setClientInstance(settings.clientInstance()).setLibraryVersion(LIBRARY_VERSION)
                .addAllAcceptedCodecs(settings.acceptedCodecs()).setKeepAlive(settings.keepAlive()).setLiveUpdates(liveUpdates).build();
    }

    private boolean dataVersionAcceptable(SnapshotInfo current) {
        long version = current.getDataVersion();
        if (settings.expectedDataVersion() != null && version != settings.expectedDataVersion()) {
            log.warn("data version {} differs from the expected {}; trying the next host", version, settings.expectedDataVersion());
            return false;
        }
        if (settings.minDataVersion() != null && version < settings.minDataVersion()) {
            log.warn("data version {} is below the minimum {}; trying the next host", version, settings.minDataVersion());
            return false;
        }
        return true;
    }

    private LoadedSnapshot transfer(ServerConnection connection, HelloAck ack) throws IOException, Reconnect, ChunkIntegrityFailure, SnapshotCorruption {
        Duration heartbeat = Duration.ofMillis(ack.getHeartbeatIntervalMs());
        Duration readTimeout = settings.readTimeout();
        Duration needed = heartbeat.multipliedBy(Framing.HEARTBEAT_MISSES).plusSeconds(1);
        if (heartbeat.isPositive() && readTimeout.compareTo(needed) < 0) {
            log.info("read timeout {} raised to {} to cover {} heartbeat intervals of {}", readTimeout, needed, Framing.HEARTBEAT_MISSES, heartbeat);
            readTimeout = needed;
        }
        byte[] current = ack.getCurrentSnapshot().getSnapshotId().toByteArray();
        if (staging != null && !Arrays.equals(staging.snapshotId(), current)) {
            log.warn("{} serves snapshot {} but {} was partly received; discarding staging and starting over",
                    connection.endpoint(), Digests.hex(current), Digests.hex(staging.snapshotId()));
            discardStaging();
        }
        boolean resume = staging != null;
        boolean liveResume = !resume && livePosition != null && !ack.getUpdateGeneration().isEmpty()
                && ack.getUpdateGeneration().equals(livePosition.getGeneration());
        if (!resume && livePosition != null && !liveResume) {
            log.info("{} has another update generation (restarted or another instance); loading a full snapshot", connection.endpoint());
        }
        currentRequestId = nextRequestId++;
        MDC.put("rds.request", Long.toString(currentRequestId));
        SnapshotRequest.Builder request = SnapshotRequest.newBuilder().setRequestId(currentRequestId).addAllEntityTypes(settings.entityTypes());
        if (resume) {
            request.setResumeSnapshotId(ByteString.copyFrom(staging.snapshotId())).setResumeFromSeq(staging.nextSeq());
            stats.resumes++;
            log.info("resuming snapshot {} from seq {} at {}", Digests.hex(staging.snapshotId()), staging.nextSeq(), connection.endpoint());
        } else if (liveResume) {
            request.setLiveResume(livePosition);
            log.info("asking {} to resume live updates from data version {}", connection.endpoint(), livePosition.getDataVersion());
        } else if (knownSnapshot != null) {
            request.setIfSnapshotId(knownSnapshot.info().getSnapshotId());
        }
        connection.send(Envelope.newBuilder().setSnapshotRequest(request).build());
        stats.phase(LoadPhase.QUEUED);

        while (true) {
            Envelope frame = read(connection, readTimeout);
            switch (frame.getBodyCase()) {
                case HEARTBEAT -> {
                    if (settings.keepAlive()) {
                        connection.send(Envelope.newBuilder().setHeartbeat(Heartbeat.newBuilder().setSentAtEpochMs(System.currentTimeMillis())).build());
                    }
                }
                case SNAPSHOT_QUEUED -> {
                    SnapshotQueued q = frame.getSnapshotQueued();
                    stats.queued = true;
                    log.info("request {} queued at {} (position {}, at most {} ms)", q.getRequestId(), connection.endpoint(), q.getQueuePosition(), q.getMaxWaitMs());
                }
                case ERROR -> handleError(frame.getError(), resume);
                case GO_AWAY -> {
                    GoAway g = frame.getGoAway();
                    throw new Reconnect("GoAway " + g.getReason() + ": " + g.getMessage(), true,
                            g.getReconnectAfterMs() > 0 ? Duration.ofMillis(g.getReconnectAfterMs()) : null);
                }
                case UPDATES_RESUMED -> {
                    UpdatesResumed resumed = frame.getUpdatesResumed();
                    if (!liveResume || resumed.getRequestId() != currentRequestId || resumed.getDataVersion() != livePosition.getDataVersion()) {
                        throw new ProtocolException("unexpected UpdatesResumed for request " + resumed.getRequestId() + " at data version " + resumed.getDataVersion());
                    }
                    if (!heartbeat.isPositive()) throw new ProtocolException("live session without a heartbeat interval");
                    log.info("resumed live updates from data version {} at {} (server is at {})", resumed.getDataVersion(), connection.endpoint(),
                            resumed.getCurrentSnapshot().getDataVersion());
                    backoff.reset();
                    resumedLive = true;
                    updateGeneration = ack.getUpdateGeneration();
                    keepAliveSession = new KeepAliveSession(connection, heartbeat, updateListener);
                    return liveHeld;
                }
                case SNAPSHOT_BEGIN -> {
                    SnapshotBegin begin = frame.getSnapshotBegin();
                    if (begin.getRequestId() != currentRequestId) {
                        throw new ProtocolException("SnapshotBegin for request " + begin.getRequestId() + ", expected " + currentRequestId);
                    }
                    if (begin.getNotModified()) return notModified(connection, readTimeout);
                    onBegin(begin, connection.endpoint(), resume);
                    backoff.reset();
                    stats.phase(LoadPhase.STREAMING);
                }
                case SNAPSHOT_CHUNK -> {
                    if (staging == null) throw new ProtocolException("SnapshotChunk before SnapshotBegin");
                    SnapshotChunk chunk = frame.getSnapshotChunk();
                    staging.accept(chunk);
                    stats.chunksAccepted++;
                    stats.recordsAccepted += chunk.getRecordCount();
                }
                case SNAPSHOT_END -> {
                    if (staging == null) throw new ProtocolException("SnapshotEnd before SnapshotBegin");
                    SnapshotEnd end = frame.getSnapshotEnd();
                    if (end.getRequestId() != currentRequestId) {
                        throw new ProtocolException("SnapshotEnd for request " + end.getRequestId() + ", expected " + currentRequestId);
                    }
                    staging.verifyEnd(end, settings.unfiltered());
                    stats.skippedUnknownTypeChunks = staging.skippedUnknownTypeChunks();
                    LoadedSnapshot loaded = publish();
                    updateGeneration = ack.getUpdateGeneration();
                    snapshotListener.accept(loaded);
                    if (settings.keepAlive() && heartbeat.isPositive()) keepAliveSession = new KeepAliveSession(connection, heartbeat, updateListener);
                    return loaded;
                }
                case BODY_NOT_SET -> log.debug("ignoring envelope with no known body");
                default -> throw new ProtocolException("unexpected " + frame.getBodyCase() + " during a snapshot transfer");
            }
        }
    }

    private void onBegin(SnapshotBegin begin, Endpoint endpoint, boolean resume) throws Reconnect {
        SnapshotInfo info = begin.getSnapshot();
        MDC.put("rds.snapshot", Digests.hex(info.getSnapshotId().toByteArray()));
        if (resume) {
            if (!Arrays.equals(info.getSnapshotId().toByteArray(), staging.snapshotId()) || begin.getFirstSeq() != staging.nextSeq()) {
                discardStaging();
                throw new Reconnect("resume answered with snapshot " + Digests.hex(info.getSnapshotId().toByteArray()) + " from seq "
                        + begin.getFirstSeq() + "; starting over", false, null);
            }
            log.info("resumed at seq {} from {}", begin.getFirstSeq(), endpoint);
            return;
        }
        List<Integer> sentTypes = begin.getEntityTypesCount() > 0 ? begin.getEntityTypesList()
                : info.getEntitiesList().stream().map(m -> m.getEntityType()).toList();
        staging = new SnapshotStaging(info, sentTypes, settings.validationPolicy());
        if (!staging.complete() && begin.getFirstSeq() != staging.nextSeq()) {
            discardStaging();
            throw new ProtocolException("SnapshotBegin starts at seq " + begin.getFirstSeq() + " but the manifest implies " + staging.nextSeq());
        }
        log.info("snapshot {} (data version {}, {} records in {} chunks; this transfer: {} chunks of types {}) from {}",
                Digests.hex(staging.snapshotId()), info.getDataVersion(), info.getTotalRecords(), info.getTotalChunks(),
                staging.expectedChunks(), sentTypes, endpoint);
    }

    /** Maps an ErrorReply received after the handshake to the recovery rule it triggers; returns for non-fatal noise. */
    private void handleError(ErrorReply error, boolean resume) throws Reconnect {
        String text = error.getCode() + ": " + error.getMessage();
        switch (error.getCode()) {
            case ERROR_CODE_SERVER_BUSY -> throw new ServerBusyException(text, Duration.ofMillis(error.getRetryAfterMs()));
            case ERROR_CODE_UNSUPPORTED_PROTOCOL_VERSION, ERROR_CODE_UNSUPPORTED_SCHEMA_VERSION, ERROR_CODE_FRAME_TOO_LARGE ->
                    throw new UnsupportedVersionException("rejected, " + text, error);
            case ERROR_CODE_SNAPSHOT_NOT_AVAILABLE -> {
                discardStaging();
                throw new Reconnect(text + "; requesting a full snapshot", false, null);
            }
            case ERROR_CODE_INVALID_REQUEST -> {
                if (resume) {
                    discardStaging();
                    throw new Reconnect("resume refused, " + text + "; requesting a full snapshot", false, null);
                }
                throw new ProtocolException("request refused, " + text);
            }
            case ERROR_CODE_UNSUPPORTED_MESSAGE -> log.warn("server did not understand a message: {}", error.getMessage());
            default -> throw new Reconnect(text, true, error.getRetryAfterMs() > 0 ? Duration.ofMillis(error.getRetryAfterMs()) : null);
        }
    }

    private LoadedSnapshot notModified(ServerConnection connection, Duration readTimeout) throws IOException, Reconnect {
        if (knownSnapshot == null) throw new ProtocolException("SnapshotBegin says not_modified but no if_snapshot_id was sent");
        while (true) {
            Envelope frame = read(connection, readTimeout);
            switch (frame.getBodyCase()) {
                case HEARTBEAT -> { }
                case ERROR -> handleError(frame.getError(), false);
                case SNAPSHOT_END -> {
                    log.info("snapshot {} unchanged; keeping the snapshot already held", Digests.hex(knownSnapshot.info().getSnapshotId().toByteArray()));
                    return new LoadedSnapshot(knownSnapshot.store(), knownSnapshot.info(), stats.snapshot());
                }
                default -> throw new ProtocolException("expected SnapshotEnd after not_modified, got " + frame.getBodyCase());
            }
        }
    }

    private LoadedSnapshot publish() {
        stats.phase(LoadPhase.INDEXING);
        RefDataStoreBuilder.Result result;
        try {
            result = staging.builder().build();
        } catch (ValidationException e) {
            throw new RefDataClientException("snapshot " + Digests.hex(staging.snapshotId()) + " failed validation: " + e.getMessage(), e);
        }
        if (!result.report().isClean()) log.warn("snapshot published with quarantined rows: {}", result.report());
        LoadStats loadStats = stats.snapshot();
        log.info("snapshot {} loaded: {} records, {} chunks, {} bytes in {} ({} connections, {} resumes, {} restarts)",
                Digests.hex(staging.snapshotId()), loadStats.recordsAccepted(), loadStats.chunksAccepted(), loadStats.bytesReceived(),
                loadStats.totalDuration(), loadStats.connections(), loadStats.resumes(), loadStats.fullRestarts());
        return new LoadedSnapshot(result.store(), staging.info(), loadStats);
    }

    // ------------------------------------------------------------------------------------------------ housekeeping

    private Envelope read(ServerConnection connection, Duration readTimeout) throws IOException {
        Duration remaining = Duration.ofNanos(Math.max(1, deadlineNanos - System.nanoTime()));
        connection.readTimeout(readTimeout.compareTo(remaining) < 0 ? readTimeout : remaining);
        long before = connection.bytesRead();
        Envelope frame = connection.read();
        stats.bytesReceived += connection.bytesRead() - before;
        checkDeadline(null);
        return frame;
    }

    private void discardStaging() {
        if (staging == null) return;
        staging = null;
        stats.fullRestarts++;
    }

    /** A C-4 failure: discard the staging area; give up once {@link #MAX_FULL_FAILURES} is reached. */
    private void fullFailure(String message, Exception cause) {
        fullFailures++;
        discardStaging();
        if (fullFailures >= MAX_FULL_FAILURES) {
            throw new SnapshotIntegrityException("giving up after " + fullFailures + " failed snapshot transfers; last: " + message, cause);
        }
        log.warn("snapshot transfer {} of {} failed, starting over: {}", fullFailures, MAX_FULL_FAILURES, message);
    }

    private void pause(Duration delay, Exception lastFailure) {
        checkDeadline(lastFailure);
        long remaining = deadlineNanos - System.nanoTime();
        long sleepNanos = Math.min(delay.toNanos(), remaining);
        try {
            Thread.sleep(Duration.ofNanos(sleepNanos));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RefDataUnavailableException(stats.phase(), "interrupted while waiting to reconnect", lastFailure);
        }
        checkDeadline(lastFailure);
    }

    private void checkDeadline(Exception lastFailure) {
        if (System.nanoTime() - deadlineNanos < 0) return;
        String detail = lastFailure == null ? "" : "; last failure: " + (lastFailure.getMessage() == null ? lastFailure.toString() : lastFailure.getMessage());
        throw new RefDataUnavailableException(stats.phase(), "startup timeout of " + settings.startupTimeout() + " expired after "
                + stats.connections + " connection attempt(s)" + detail, lastFailure);
    }
}
