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
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Milestone 10 on the session state machine: bounded lag for live consumers, resynchronisation, and its metrics. */
public class SessionHandlerLiveDeliveryTest {
    private static final Snapshot SNAPSHOT = new SnapshotEncoder(2048, 2048 + Framing.CHUNK_HEADROOM_BYTES, Set.of())
            .encode(TestStores.sized(20), "test", 0);
    private static final long V = SNAPSHOT.dataVersion();
    private static final ByteString GENERATION = ByteString.copyFromUtf8("generation-0001");

    private final ServerMetrics metrics = new ServerMetrics();
    private final List<SessionHandler.LiveUpdate> history = new ArrayList<>();
    private long latest = V;

    private final SessionHandler.SessionHost host = new SessionHandler.SessionHost() {
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
    };

    private void publish(long version) {
        history.add(new SessionHandler.LiveUpdate(version - 1, version, update(version)));
        latest = version;
    }

    private static Envelope update(long version) {
        return Envelope.newBuilder().setUpdateBatch(ByteString.copyFromUtf8("batch-" + version)).build();
    }

    /** A live session that resumed at {@link #V} and is waiting for updates. */
    private EmbeddedChannel liveSession(String maxLag) {
        Properties p = new Properties();
        p.setProperty("rds.heartbeat.interval", "1s");
        p.setProperty("rds.snapshot.queue.max.wait", "5s");
        p.setProperty("rds.live.max.lag", maxLag);
        ServerConfig config = ServerConfig.fromProperties(p);
        AdmissionController admission = new AdmissionController(config.maxConcurrentSnapshots, config.snapshotQueueSize);
        EmbeddedChannel ch = new EmbeddedChannel(new SessionHandler(config, SNAPSHOT, admission, host, metrics, "srv", "test"));
        ch.writeInbound(Envelope.newBuilder().setHello(Hello.newBuilder().setProtocolMajor(1).addSchemaMajors(1).setClientName("live")
                .setKeepAlive(true).setLiveUpdates(true)).build());
        assertTrue(env(ch).getHelloAck().getLiveUpdates());
        ch.writeInbound(Envelope.newBuilder().setSnapshotRequest(SnapshotRequest.newBuilder().setRequestId(1)
                .setLiveResume(LiveResume.newBuilder().setGeneration(GENERATION).setDataVersion(V))).build());
        assertEquals(Envelope.BodyCase.UPDATES_RESUMED, env(ch).getBodyCase());
        assertEquals(1, metrics.liveSessionsActive());
        return ch;
    }

    /** Simulates a consumer that is not reading: the channel reports itself above the write-buffer high-water mark. */
    private static void writable(EmbeddedChannel ch, boolean writable) {
        ch.unsafe().outboundBuffer().setUserDefinedWritability(1, writable);
        ch.runPendingTasks();                                      // the writability event is dispatched on the event loop
        assertEquals(writable, ch.isWritable());
    }

    private static void deliver(EmbeddedChannel ch) {
        ch.pipeline().get(SessionHandler.class).deliverLiveUpdates();
    }

    private static Envelope env(EmbeddedChannel ch) {
        Object o;
        do { o = ch.readOutbound(); } while (o instanceof Envelope e && e.hasHeartbeat());
        assertTrue("expected an Envelope, got " + o, o instanceof Envelope);
        return (Envelope) o;
    }

    private static Object nextNonHeartbeat(EmbeddedChannel ch) {
        Object o;
        do { o = ch.readOutbound(); } while (o instanceof Envelope e && e.hasHeartbeat());
        return o;
    }

    @Test public void briefLagIsToleratedAndTheBacklogIsSentInOrderOnceWritable() {
        EmbeddedChannel ch = liveSession("10s");
        writable(ch, false);
        publish(V + 1);
        publish(V + 2);
        deliver(ch);
        assertNull("nothing is written past the high-water mark", nextNonHeartbeat(ch));
        assertTrue(ch.isOpen());
        assertEquals(1, metrics.liveSessionsLagging());

        writable(ch, true);                                        // the writability event resumes delivery without a push
        assertEquals(update(V + 1), env(ch));
        assertEquals(update(V + 2), env(ch));
        assertNull(nextNonHeartbeat(ch));
        assertEquals(0, metrics.liveSessionsLagging());
        assertEquals(2, metrics.liveUpdatesSent());
        assertTrue(ch.isOpen());
        ch.close();
        assertEquals(0, metrics.liveSessionsActive());
    }

    @Test public void consumerLaggingLongerThanTheLimitIsDisconnectedWithSlowConsumer() throws Exception {
        EmbeddedChannel ch = liveSession("100ms");
        writable(ch, false);
        publish(V + 1);
        deliver(ch);                                               // the lag clock starts
        assertTrue(ch.isOpen());
        Thread.sleep(150);
        ch.runScheduledPendingTasks();                             // the 50 ms poll notices the deadline on its own
        Envelope error = env(ch);
        assertEquals(ErrorCode.ERROR_CODE_SLOW_CONSUMER, error.getError().getCode());
        assertTrue(error.getError().getFatal());
        assertTrue(error.getError().getMessage(), error.getError().getMessage().contains("1 update(s) behind"));
        ch.runPendingTasks();
        assertFalse(ch.isOpen());
        assertEquals(1, metrics.liveResyncs("slow_consumer"));
        assertEquals(0, metrics.liveSessionsLagging());
        assertEquals(0, metrics.liveSessionsActive());
        assertEquals(0, metrics.liveUpdatesSent());
    }

    @Test public void catchingUpResetsTheLagClock() throws Exception {
        EmbeddedChannel ch = liveSession("200ms");
        writable(ch, false);
        publish(V + 1);
        deliver(ch);
        Thread.sleep(120);
        writable(ch, true);
        assertEquals(update(V + 1), env(ch));
        writable(ch, false);
        publish(V + 2);
        deliver(ch);
        Thread.sleep(120);                                         // 240 ms lagging in total, but never 200 ms in one stretch
        deliver(ch);
        assertTrue(ch.isOpen());
        assertNull(nextNonHeartbeat(ch));
        assertEquals(0, metrics.liveResyncs("slow_consumer"));
        ch.close();
    }

    @Test public void historyExpiryIsDetectedEvenWhileTheConsumerIsNotReading() {
        EmbeddedChannel ch = liveSession("10s");
        writable(ch, false);
        publish(V + 1);
        publish(V + 2);
        history.remove(0);                                         // V -> V+1 expired before it could be sent
        deliver(ch);
        Envelope error = env(ch);
        assertEquals(ErrorCode.ERROR_CODE_SNAPSHOT_NOT_AVAILABLE, error.getError().getCode());
        assertTrue(error.getError().getFatal());
        ch.runPendingTasks();
        assertFalse(ch.isOpen());
        assertEquals(1, metrics.liveResyncs("history_expired"));
        assertEquals(0, metrics.liveSessionsLagging());
    }

    @Test public void idleConsumerIsNotLaggingWhileNothingIsDue() throws Exception {
        EmbeddedChannel ch = liveSession("50ms");
        writable(ch, false);
        deliver(ch);
        Thread.sleep(80);
        deliver(ch);
        assertTrue(ch.isOpen());
        assertEquals(0, metrics.liveSessionsLagging());
        ch.close();
    }

    @Test public void metricsAreRendered() {
        EmbeddedChannel ch = liveSession("10s");
        publish(V + 1);
        deliver(ch);
        assertEquals(update(V + 1), env(ch));
        String text = metrics.render();
        assertTrue(text, text.contains("rds_live_sessions_active 1"));
        assertTrue(text, text.contains("rds_live_sessions_lagging 0"));
        assertTrue(text, text.contains("rds_live_updates_sent_total 1"));
        assertTrue(text, text.contains("# TYPE rds_live_resyncs_total counter"));
        ch.close();
    }
}
