package com.example.refdata.server;

import com.example.refdata.model.EntityKind;
import com.example.refdata.protocol.v1.Codec;
import org.junit.After;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ServerConfigTest {
    @After public void clearOverrides() {
        for (String k : List.of("rds.tcp.port", "rds.codecs", "rds.db.password")) System.clearProperty(k);
    }

    @Test public void defaultsMatchDesignSection16() {
        ServerConfig c = ServerConfig.load();
        assertEquals(7400, c.tcpPort);
        assertEquals("0.0.0.0", c.bindAddress);
        assertEquals(0, c.tcpSendBuffer);
        assertEquals(Runtime.getRuntime().availableProcessors(), c.ioThreads);
        assertEquals(7401, c.httpPort);
        assertEquals(5, c.dbConnectRetries);
        assertEquals(10_000, c.loadFetchSize);
        assertEquals(4, c.loadParallelism);
        assertEquals(2, c.loadRetries);
        assertEquals(com.example.refdata.model.validation.ValidationPolicy.FAIL, c.validationPolicy);
        assertEquals(1.5, c.indexDenseThreshold, 0.0);
        assertEquals(262_144, c.chunkTargetBytes);
        assertEquals(1_048_576, c.frameMaxBytes);
        assertTrue(c.codecs.isEmpty());
        assertEquals(256, c.maxConnections);
        assertEquals(8, c.maxConcurrentSnapshots);
        assertEquals(64, c.snapshotQueueSize);
        assertEquals(Duration.ofSeconds(60), c.snapshotQueueMaxWait);
        assertEquals(8 << 20, c.writeBufferHighWater);
        assertEquals(4 << 20, c.writeBufferLowWater);
        assertEquals(Duration.ofSeconds(10), c.handshakeTimeout);
        assertEquals(Duration.ofSeconds(60), c.sessionIdleTimeout);
        assertEquals(Duration.ofSeconds(30), c.stallTimeout);
        assertEquals(Duration.ofMinutes(10), c.snapshotMaxDuration);
        assertEquals(Duration.ofSeconds(5), c.heartbeatInterval);
        assertEquals(Duration.ofSeconds(30), c.shutdownDrainTimeout);
        for (EntityKind k : EntityKind.values()) assertEquals(1L, (long) c.tableMinRows.get(k));
    }

    @Test public void fileValuesAndSystemPropertyOverrides() throws Exception {
        Path f = Files.createTempFile("rds", ".properties");
        Files.writeString(f, "rds.tcp.port=9000\nrds.http.port=9001\nrds.chunk.target.bytes=128k\nrds.frame.max.bytes=512KiB\n"
                + "rds.snapshot.queue.max.wait=2min\nrds.stall.timeout=1500\nrds.codecs=lz4, zstd\nrds.db.url=jdbc:derby:memory:x\n"
                + "rds.db.password=secret\nrds.table.min.rows.client=100\nrds.validation.policy=quarantine\n");
        System.setProperty("rds.tcp.port", "9100");
        System.setProperty("rds.codecs", "ZSTD");
        ServerConfig c = ServerConfig.load(f);
        assertEquals(9100, c.tcpPort);
        assertEquals(9001, c.httpPort);
        assertEquals(128 << 10, c.chunkTargetBytes);
        assertEquals(512 << 10, c.frameMaxBytes);
        assertEquals(Duration.ofMinutes(2), c.snapshotQueueMaxWait);
        assertEquals(Duration.ofMillis(1500), c.stallTimeout);
        assertEquals(List.of(Codec.CODEC_ZSTD), List.copyOf(c.codecs));
        assertEquals("secret", c.dbPassword());
        assertEquals(100L, (long) c.tableMinRows.get(EntityKind.CLIENT));
        assertEquals(com.example.refdata.model.validation.ValidationPolicy.QUARANTINE, c.validationPolicy);
        String s = c.toString();
        assertFalse("password must not be printed", s.contains("secret"));
        assertTrue(s.contains("rds.db.password=****"));
        assertTrue(s.contains("rds.tcp.port=9100"));
    }

    @Test public void chunkTargetPlusHeadroomMustFitTheFrame() {
        Properties p = new Properties();
        p.setProperty("rds.chunk.target.bytes", "900k");
        p.setProperty("rds.frame.max.bytes", "960k");
        try { ServerConfig.fromProperties(p); fail(); }
        catch (ServerConfig.ConfigException e) { assertTrue(e.getMessage(), e.getMessage().contains("rds.frame.max.bytes")); }
    }

    @Test public void badValuesNameTheKey() {
        expectBad("rds.tcp.port", "abc", "rds.tcp.port");
        expectBad("rds.chunk.target.bytes", "1k", "rds.chunk.target.bytes");
        expectBad("rds.codecs", "gzip", "rds.codecs");
        expectBad("rds.heartbeat.interval", "5 parsecs", "rds.heartbeat.interval");
        expectBad("rds.write.buffer.low.water", "16m", "rds.write.buffer.low.water");
        expectBad("rds.validation.policy", "maybe", "rds.validation.policy");
        expectBad("rds.max.connections", "0", "rds.max.connections");
        expectBad("rds.tcp.port", "70000", "rds.tcp.port");
        expectBad("rds.http.port", "7400", "rds.http.port");
    }

    @Test public void missingFileIsAClearError() {
        try { ServerConfig.load(Path.of("/nonexistent/rds.properties")); fail(); }
        catch (ServerConfig.ConfigException e) { assertTrue(e.getMessage().contains("/nonexistent/rds.properties")); }
    }

    private static void expectBad(String key, String value, String expectedInMessage) {
        Properties p = new Properties();
        p.setProperty(key, value);
        try { ServerConfig.fromProperties(p); fail("expected rejection of " + key + "=" + value); }
        catch (ServerConfig.ConfigException e) { assertTrue(e.getMessage(), e.getMessage().contains(expectedInMessage)); }
    }
}
