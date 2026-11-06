package com.example.refdata.server.net;

import com.example.refdata.contract.Framing;
import com.example.refdata.protocol.v1.Codec;
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
import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Milestone 9 on the session state machine: resuming a live subscription from a data version, and the fallbacks. */
public class SessionHandlerLiveResumeTest {
    private static final Snapshot SNAPSHOT = new SnapshotEncoder(2048, 2048 + Framing.CHUNK_HEADROOM_BYTES, Set.of(Codec.CODEC_LZ4))
            .encode(TestStores.sized(60), "test", 0);
    private static final long V = SNAPSHOT.dataVersion();
    private static final ByteString GENERATION = ByteString.copyFromUtf8("generation-0001");

    private ServerConfig config;
    private AdmissionController admission;
    private ServerMetrics metrics;

    /** A live host whose history and latest version the test controls; mirrors RefDataTcpServer's rules. */
    private final class LiveHost implements SessionHandler.SessionHost {
        final List<SessionHandler.LiveUpdate> history = new ArrayList<>();
        long latest = V;
        void publish(long version) {
            history.add(new SessionHandler.LiveUpdate(version - 1, version, update(version)));
            latest = version;
        }
        @Override public boolean acceptingTransfers() { return true; }
        @Override public boolean liveUpdatesEnabled() { return true; }
        @Override public void storedFrameCorrupt(int seq, Codec codec) { }
        @Override public ByteString updateGeneration() { return GENERATION; }
        @Override public boolean canResumeFrom(long version) {
            return version == latest || history.stream().anyMatch(u -> u.baseVersion() == version);
        }
        @Override public List<SessionHandler.LiveUpdate> updatesAfter(long afterVersion) {
            return history.stream().filter(u -> u.version() > afterVersion).toList();
        }
    }

    private final LiveHost host = new LiveHost();

    @Before public void setUp() {
        Properties p = new Properties();
        p.setProperty("rds.heartbeat.interval", "1s");
        p.setProperty("rds.snapshot.queue.max.wait", "5s");
        config = ServerConfig.fromProperties(p);
        admission = new AdmissionController(config.maxConcurrentSnapshots, config.snapshotQueueSize);
        metrics = new ServerMetrics();
    }

    private static Envelope update(long version) {
        return Envelope.newBuilder().setUpdateBatch(ByteString.copyFromUtf8("batch-" + version)).build();
    }

    private EmbeddedChannel liveSession(SessionHandler.SessionHost h) {
        EmbeddedChannel ch = new EmbeddedChannel(new SessionHandler(config, SNAPSHOT, admission, h, metrics, "srv", "test"));
        ch.writeInbound(Envelope.newBuilder().setHello(Hello.newBuilder().setProtocolMajor(1).addSchemaMajors(1).setClientName("live")
                .setKeepAlive(true).setLiveUpdates(true)).build());
        Envelope ack = env(ch);
        assertTrue(ack.getHelloAck().getLiveUpdates());
        return ch;
    }

    private static Envelope resumeRequest(long id, ByteString generation, long version) {
        return Envelope.newBuilder().setSnapshotRequest(SnapshotRequest.newBuilder().setRequestId(id)
                .setLiveResume(LiveResume.newBuilder().setGeneration(generation).setDataVersion(version))).build();
    }

    private static Envelope env(EmbeddedChannel ch) {
        Object o;
        do { o = ch.readOutbound(); } while (o instanceof Envelope e && e.hasHeartbeat());
        assertTrue("expected an Envelope, got " + o, o instanceof Envelope);
        return (Envelope) o;
    }

    /** Reads a full snapshot transfer (Begin, chunk frames, End) and returns the SnapshotEnd envelope. */
    private static Envelope fullSnapshot(EmbeddedChannel ch, long requestId) {
        Envelope begin = env(ch);
        assertEquals(Envelope.BodyCase.SNAPSHOT_BEGIN, begin.getBodyCase());
        assertEquals(requestId, begin.getSnapshotBegin().getRequestId());
        for (int i = 0; i < SNAPSHOT.chunkCount(); i++) {
            Object o = ch.readOutbound();
            assertTrue("chunk " + i, o instanceof ByteBuf);
            ((ByteBuf) o).release();
        }
        Envelope end = env(ch);
        assertEquals(Envelope.BodyCase.SNAPSHOT_END, end.getBodyCase());
        return end;
    }

    @Test public void helloAckOffersTheUpdateGenerationToLiveSessionsOnly() {
        EmbeddedChannel live = liveSession(host);
        live.close();
        EmbeddedChannel plain = new EmbeddedChannel(new SessionHandler(config, SNAPSHOT, admission, host, metrics, "srv", "test"));
        plain.writeInbound(Envelope.newBuilder().setHello(Hello.newBuilder().setProtocolMajor(1).addSchemaMajors(1).setClientName("plain").setKeepAlive(true)).build());
        assertTrue(env(plain).getHelloAck().getUpdateGeneration().isEmpty());
        plain.close();

        EmbeddedChannel again = new EmbeddedChannel(new SessionHandler(config, SNAPSHOT, admission, host, metrics, "srv", "test"));
        again.writeInbound(Envelope.newBuilder().setHello(Hello.newBuilder().setProtocolMajor(1).addSchemaMajors(1).setClientName("live")
                .setKeepAlive(true).setLiveUpdates(true)).build());
        assertEquals(GENERATION, env(again).getHelloAck().getUpdateGeneration());
        again.close();
    }

    @Test public void resumeFromARetainedVersionReplaysTheMissedUpdatesInOrderWithoutASnapshot() {
        host.publish(V + 1);
        host.publish(V + 2);
        host.publish(V + 3);
        EmbeddedChannel ch = liveSession(host);
        ch.writeInbound(resumeRequest(5, GENERATION, V + 1));
        Envelope resumed = env(ch);
        assertEquals(Envelope.BodyCase.UPDATES_RESUMED, resumed.getBodyCase());
        assertEquals(5, resumed.getUpdatesResumed().getRequestId());
        assertEquals(V + 1, resumed.getUpdatesResumed().getDataVersion());
        assertEquals(SNAPSHOT.info(), resumed.getUpdatesResumed().getCurrentSnapshot());
        assertEquals(update(V + 2), env(ch));                      // V+1 is already held: never sent again
        assertEquals(update(V + 3), env(ch));
        assertNull(ch.readOutbound());
        assertEquals(0, metrics.transfersStarted());
        assertEquals(0, admission.active());

        host.publish(V + 4);                                        // later publications keep flowing on the session
        ch.pipeline().get(SessionHandler.class).deliverLiveUpdates();
        assertEquals(update(V + 4), env(ch));
        ch.close();
    }

    @Test public void resumeAtTheLatestVersionWaitsForTheNextPublication() {
        EmbeddedChannel ch = liveSession(host);
        ch.writeInbound(resumeRequest(1, GENERATION, V));
        assertEquals(Envelope.BodyCase.UPDATES_RESUMED, env(ch).getBodyCase());
        assertNull(ch.readOutbound());
        host.publish(V + 1);
        ch.pipeline().get(SessionHandler.class).deliverLiveUpdates();
        assertEquals(update(V + 1), env(ch));
        ch.close();
    }

    @Test public void otherGenerationFallsBackToAFullSnapshotFollowedByUpdates() {
        EmbeddedChannel ch = liveSession(host);
        ch.writeInbound(resumeRequest(3, ByteString.copyFromUtf8("restarted-server"), V));
        fullSnapshot(ch, 3);
        host.publish(V + 1);
        ch.pipeline().get(SessionHandler.class).deliverLiveUpdates();
        assertEquals(update(V + 1), env(ch));                      // the session's snapshot version is the delivery base
        ch.close();
    }

    @Test public void versionOutsideTheRetainedHistoryFallsBackToAFullSnapshot() {
        host.publish(V + 1);
        host.publish(V + 2);
        host.history.remove(0);                                    // expired: only V+1 -> V+2 is retained
        EmbeddedChannel ch = liveSession(host);
        ch.writeInbound(resumeRequest(4, GENERATION, V - 5));       // a gap: too old
        fullSnapshot(ch, 4);
        ch.close();
    }

    @Test public void versionAheadOfTheServerFallsBackToAFullSnapshot() {
        EmbeddedChannel ch = liveSession(host);
        ch.writeInbound(resumeRequest(2, GENERATION, V + 10));
        fullSnapshot(ch, 2);
        ch.close();
    }

    @Test public void historyExpiringBetweenResumeAndDeliveryClosesTheSessionForResynchronisation() {
        host.publish(V + 1);
        EmbeddedChannel ch = liveSession(host);
        ch.writeInbound(resumeRequest(1, GENERATION, V + 1));
        assertEquals(Envelope.BodyCase.UPDATES_RESUMED, env(ch).getBodyCase());
        host.publish(V + 2);
        host.publish(V + 3);
        host.history.removeIf(u -> u.version() <= V + 2);          // V+1 -> V+2 dropped before it could be sent
        ch.pipeline().get(SessionHandler.class).deliverLiveUpdates();
        Envelope error = env(ch);
        assertEquals(ErrorCode.ERROR_CODE_SNAPSHOT_NOT_AVAILABLE, error.getError().getCode());
        assertTrue(error.getError().getFatal());
        ch.runPendingTasks();
        assertFalse(ch.isOpen());
    }

    @Test public void secondRequestAfterAResumeIsRejected() {
        EmbeddedChannel ch = liveSession(host);
        ch.writeInbound(resumeRequest(1, GENERATION, V));
        assertEquals(Envelope.BodyCase.UPDATES_RESUMED, env(ch).getBodyCase());
        ch.writeInbound(resumeRequest(2, GENERATION, V));
        assertEquals(ErrorCode.ERROR_CODE_INVALID_REQUEST, env(ch).getError().getCode());
    }

    @Test public void liveResumeWithoutALiveSubscriptionIsInvalid() {
        EmbeddedChannel ch = new EmbeddedChannel(new SessionHandler(config, SNAPSHOT, admission, host, metrics, "srv", "test"));
        ch.writeInbound(Envelope.newBuilder().setHello(Hello.newBuilder().setProtocolMajor(1).addSchemaMajors(1).setClientName("plain").setKeepAlive(true)).build());
        env(ch);
        ch.writeInbound(resumeRequest(1, GENERATION, V));
        Envelope error = env(ch);
        assertEquals(ErrorCode.ERROR_CODE_INVALID_REQUEST, error.getError().getCode());
        assertTrue(error.getError().getFatal());
    }

    @Test public void liveResumeCombinedWithASnapshotResumeIsInvalid() {
        EmbeddedChannel ch = liveSession(host);
        ch.writeInbound(Envelope.newBuilder().setSnapshotRequest(SnapshotRequest.newBuilder().setRequestId(1)
                .setResumeSnapshotId(SNAPSHOT.info().getSnapshotId()).setResumeFromSeq(1)
                .setLiveResume(LiveResume.newBuilder().setGeneration(GENERATION).setDataVersion(V))).build());
        assertEquals(ErrorCode.ERROR_CODE_INVALID_REQUEST, env(ch).getError().getCode());
    }

    @Test public void hostWithoutResumeSupportAlwaysServesASnapshot() {
        SessionHandler.SessionHost noResume = new SessionHandler.SessionHost() {
            @Override public boolean acceptingTransfers() { return true; }
            @Override public boolean liveUpdatesEnabled() { return true; }
            @Override public void storedFrameCorrupt(int seq, Codec codec) { }
        };
        EmbeddedChannel ch = liveSession(noResume);
        ch.writeInbound(resumeRequest(1, ByteString.EMPTY, V));
        fullSnapshot(ch, 1);
        ch.close();
    }
}
