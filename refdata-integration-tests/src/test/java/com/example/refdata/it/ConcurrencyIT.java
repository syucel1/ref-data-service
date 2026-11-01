package com.example.refdata.it;

import com.example.refdata.client.LoadedSnapshot;
import com.example.refdata.db.EmbeddedDerby;
import com.example.refdata.model.RefDataStore;
import com.example.refdata.server.RefDataServer;
import com.example.refdata.testing.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Admission control: more clients than transfer slots, everyone still gets the snapshot. */
@Category(IntegrationTest.class)
public class ConcurrencyIT {
    private static final Logger log = LoggerFactory.getLogger(ConcurrencyIT.class);
    private static final int CLIENTS = 5;

    @Test public void fiveClientsWithTwoSlotsAllLoadAndSomeAreQueued() throws Exception {
        try (EmbeddedDerby db = ItSupport.tinyDb();
             RefDataServer server = ItSupport.startServer(db, "rds.max.concurrent.snapshots", "2", "rds.heartbeat.interval", "200ms");
             TcpProxy proxy = new TcpProxy("127.0.0.1", server.tcpPort()).throttle(500_000)) {
            RefDataStore reference = ItSupport.referenceStore(db);
            List<CompletableFuture<LoadedSnapshot>> loads = new ArrayList<>();
            for (int i = 0; i < CLIENTS; i++) {
                loads.add(ItSupport.client(proxy.endpoint()).clientInstance("client-" + i).build().loadAsync());
            }
            int queued = 0;
            for (CompletableFuture<LoadedSnapshot> f : loads) {
                LoadedSnapshot loaded = f.get();
                ItSupport.assertSameStore(reference, loaded.store());
                if (loaded.stats().queued()) queued++;
                log.info("client load: queued={} total={} phases={}", loaded.stats().queued(), loaded.stats().totalDuration(), loaded.stats().durationByPhase());
            }
            assertEquals(CLIENTS, server.metrics().transfersCompleted());
            assertTrue("at least one client was queued (" + queued + ")", queued >= 1);
            assertEquals(0, server.metrics().serverBusyReplies());
            assertEquals(0, server.metrics().sessionsQueued());
        }
    }
}
