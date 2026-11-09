package com.example.refdata.server.net;

import com.example.refdata.contract.Framing;
import com.example.refdata.protocol.v1.Envelope;
import com.example.refdata.server.ServerConfig;
import com.example.refdata.server.TestStores;
import com.example.refdata.server.health.ServerMetrics;
import com.example.refdata.server.snapshot.Snapshot;
import com.example.refdata.server.snapshot.SnapshotEncoder;
import com.google.protobuf.ByteString;
import org.junit.Test;

import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Milestone 10: the shared live-update history is bounded by batch count and bytes, and resume follows the bound. */
public class LiveHistoryTest {
    private static final SnapshotEncoder ENCODER = new SnapshotEncoder(2048, 2048 + Framing.CHUNK_HEADROOM_BYTES, Set.of());
    private static final long BASE = 100;

    private static RefDataTcpServer server(String batches, String bytes) {
        Properties p = new Properties();
        p.setProperty("rds.chunk.target.bytes", "64k");
        p.setProperty("rds.frame.max.bytes", "128k");
        p.setProperty("rds.live.history.batches", batches);
        p.setProperty("rds.live.history.bytes", bytes);
        Snapshot initial = ENCODER.encode(TestStores.small(BASE), "test", 0);
        return new RefDataTcpServer(ServerConfig.fromProperties(p), initial, new ServerMetrics(), "h", "0");
    }

    private static void publish(RefDataTcpServer s, long version, int payloadBytes) {
        s.publishUpdate(ENCODER.encode(TestStores.small(version), "test", 0),
                Envelope.newBuilder().setUpdateBatch(ByteString.copyFrom(new byte[payloadBytes])).build());
    }

    private static List<Long> versions(List<SessionHandler.LiveUpdate> updates) {
        return updates.stream().map(SessionHandler.LiveUpdate::version).toList();
    }

    @Test public void historyKeepsAtMostTheConfiguredNumberOfBatches() {
        RefDataTcpServer s = server("4", "128k");
        for (long v = BASE + 1; v <= BASE + 10; v++) publish(s, v, 100);
        assertEquals(List.of(BASE + 7, BASE + 8, BASE + 9, BASE + 10), versions(s.liveHistory()));
        assertTrue(s.canResumeFrom(BASE + 6));                    // V+6 -> V+7 is retained
        assertTrue(s.canResumeFrom(BASE + 10));                   // the latest version needs no replay
        assertFalse(s.canResumeFrom(BASE + 5));                   // expired: must reload a snapshot
        assertEquals(versions(s.updatesAfter(BASE + 8)), List.of(BASE + 9, BASE + 10));
    }

    @Test public void historyKeepsAtMostTheConfiguredNumberOfBytes() {
        RefDataTcpServer s = server("100", "128k");
        for (long v = BASE + 1; v <= BASE + 10; v++) publish(s, v, 40 << 10);
        List<SessionHandler.LiveUpdate> kept = s.liveHistory();
        assertEquals("three 40 KiB batches fit 128 KiB, four do not", List.of(BASE + 8, BASE + 9, BASE + 10), versions(kept));
        assertTrue(kept.stream().mapToLong(SessionHandler.LiveUpdate::bytes).sum() <= 128 << 10);
        assertFalse(s.canResumeFrom(BASE + 6));
        assertTrue(s.canResumeFrom(BASE + 7));
    }

    @Test public void theNewestBatchIsAlwaysRetained() {
        RefDataTcpServer s = server("1", "128k");
        publish(s, BASE + 1, 120 << 10);
        publish(s, BASE + 2, 120 << 10);
        assertEquals(List.of(BASE + 2), versions(s.liveHistory()));
        assertTrue(s.canResumeFrom(BASE + 1));
    }
}
