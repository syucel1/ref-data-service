package com.example.refdata.it;

import com.example.refdata.client.LoadedSnapshot;
import com.example.refdata.client.RefDataUnavailableException;
import com.example.refdata.db.EmbeddedDerby;
import com.example.refdata.model.RefDataStore;
import com.example.refdata.protocol.v1.Codec;
import com.example.refdata.server.RefDataServer;
import com.example.refdata.testing.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** Faults between client and server (through {@link TcpProxy}) and a server shutdown mid-load. */
@Category(IntegrationTest.class)
public class FaultInjectionIT {
    private static final Logger log = LoggerFactory.getLogger(FaultInjectionIT.class);

    private EmbeddedDerby db;
    private RefDataStore reference;
    private RefDataServer server;

    @Before public void start() throws Exception {
        db = ItSupport.tinyDb();
        reference = ItSupport.referenceStore(db);
        // small chunks so that a TINY snapshot still has a good number of frames to aim at
        // small chunks and small write buffers, so the server cannot push a whole TINY snapshot into buffers at once
        server = ItSupport.startServer(db, "rds.chunk.target.bytes", "64k", "rds.shutdown.drain.timeout", "300ms",
                "rds.frame.max.bytes", "256k", "rds.write.buffer.high.water", "256k", "rds.write.buffer.low.water", "128k",
                "rds.tcp.send.buffer", "32k");
    }

    @After public void stop() {
        server.close();
        db.close();
    }

    @Test public void connectionCutMidStreamIsResumedOnTheSecondEndpoint() throws Exception {
        long total = server.snapshot().totalFrameBytes(Codec.CODEC_NONE);
        try (TcpProxy proxy = new TcpProxy("127.0.0.1", server.tcpPort()).dropAfterBytes(total / 2)) {
            LoadedSnapshot loaded = ItSupport.client(proxy.endpoint(), ItSupport.endpoint(server)).build().load();
            ItSupport.assertSameStore(reference, loaded.store());
            assertEquals(1, loaded.stats().resumes());
            assertEquals(0, loaded.stats().fullRestarts());
            assertEquals(2, loaded.stats().connections());
            assertEquals(server.snapshot().chunkCount(), loaded.stats().chunksAccepted());
            assertEquals(1, proxy.connections());
            assertEquals("original transfer plus the resume", 2, server.metrics().transfersStarted());
            log.info("resume after cut at {} of {} bytes: {}", total / 2, total, loaded.stats());
        }
    }

    @Test public void flippedPayloadByteIsReportedAndTheLoadCompletesOnTheSecondEndpoint() throws Exception {
        assertTrue("need several chunks", server.snapshot().chunkCount() >= 4);
        // server-to-client frames: 0 = HelloAck, 1 = SnapshotBegin, 2.. = chunks; corrupt the third chunk
        try (TcpProxy proxy = new TcpProxy("127.0.0.1", server.tcpPort()).corruptFrame(i -> i == 4)) {
            LoadedSnapshot loaded = ItSupport.client(proxy.endpoint(), ItSupport.endpoint(server)).build().load();
            ItSupport.assertSameStore(reference, loaded.store());
            assertEquals(1, loaded.stats().integrityFailures());
            assertEquals(1, loaded.stats().resumes());
            assertEquals(2, loaded.stats().connections());
            long until = System.nanoTime() + Duration.ofSeconds(5).toNanos();   // the report travels through the proxy after the client closed
            while (server.metrics().integrityReports() == 0 && System.nanoTime() < until) Thread.sleep(20);
            assertEquals("server counted the integrity report", 1, server.metrics().integrityReports());
            log.info("integrity failure path: {}", loaded.stats());
        }
    }

    @Test public void serverShutdownMidLoadWithASingleEndpointFailsWithinTheStartupTimeout() throws Exception {
        try (TcpProxy proxy = new TcpProxy("127.0.0.1", server.tcpPort()).throttle(100_000)) {
            var future = ItSupport.client(proxy.endpoint()).startupTimeout(Duration.ofSeconds(5)).build().loadAsync();
            Thread.sleep(800);
            long t0 = System.nanoTime();
            server.close();
            RefDataUnavailableException e = assertThrows(RefDataUnavailableException.class, () -> {
                try { future.join(); } catch (java.util.concurrent.CompletionException ce) { throw ce.getCause(); }
            });
            long waited = (System.nanoTime() - t0) / 1_000_000;
            log.info("client failed {} ms after the shutdown: {}", waited, e.getMessage());
            assertTrue("failed within the startup timeout (" + waited + " ms)", waited < 6_000);
        }
    }

    @Test public void serverShutdownMidLoadCompletesOnTheSecondServer() throws Exception {
        try (RefDataServer second = ItSupport.startServer(db, "rds.chunk.target.bytes", "64k");
             TcpProxy proxy = new TcpProxy("127.0.0.1", server.tcpPort()).throttle(100_000)) {
            var future = ItSupport.client(proxy.endpoint(), ItSupport.endpoint(second)).build().loadAsync();
            Thread.sleep(800);
            server.close();
            LoadedSnapshot loaded = future.get();
            ItSupport.assertSameStore(reference, loaded.store());
            assertTrue("transfer continued elsewhere: " + loaded.stats(), loaded.stats().resumes() + loaded.stats().fullRestarts() >= 1);
            assertEquals(1, second.metrics().transfersCompleted());
            log.info("fail-over after shutdown: {}", loaded.stats());
        }
    }
}
