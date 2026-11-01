package com.example.refdata.server.health;

import com.example.refdata.contract.Framing;
import com.example.refdata.model.EntityKind;
import com.example.refdata.model.validation.ValidationReport;
import com.example.refdata.protocol.v1.Codec;
import com.example.refdata.server.TestStores;
import com.example.refdata.server.snapshot.Snapshot;
import com.example.refdata.server.snapshot.SnapshotEncoder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.EnumMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HealthServerTest {
    private final ServerMetrics metrics = new ServerMetrics();
    private final HealthServer health = new HealthServer(metrics);

    @Before public void start() throws Exception { health.start("127.0.0.1", 0); }
    @After public void stop() { health.close(); }

    private record Http(int status, String body) {
    }

    private Http get(String path) throws Exception {
        HttpURLConnection c = (HttpURLConnection) URI.create("http://127.0.0.1:" + health.port() + path).toURL().openConnection();
        c.setConnectTimeout(2000); c.setReadTimeout(2000);
        int status = c.getResponseCode();
        try (InputStream in = status >= 400 ? c.getErrorStream() : c.getInputStream()) {
            return new Http(status, in == null ? "" : new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8));
        }
    }

    @Test public void readinessFollowsPublication() throws Exception {
        assertEquals(200, get("/health/live").status());
        health.phase("loading");
        Http ready = get("/health/ready");
        assertEquals(503, ready.status());
        assertTrue(ready.body(), ready.body().contains("loading"));
        assertEquals(503, get("/snapshot").status());
        assertEquals(503, get("/startup-report").status());

        Snapshot s = new SnapshotEncoder(64 << 10, 128 << 10, Set.of(Codec.CODEC_LZ4)).encode(TestStores.small(), "unit", 5);
        Map<EntityKind, Long> counts = new EnumMap<>(EntityKind.class);
        for (EntityKind k : EntityKind.values()) counts.put(k, (long) TestStores.small().count(k));
        health.published(s, new StartupReport("unit", counts, counts, 42, 1, 2, 3, new ValidationReport(), 10, 20, 30, "test factors", 1, s));
        health.ready(true);
        assertEquals(200, get("/health/ready").status());
        Http snap = get("/snapshot");
        assertEquals(200, snap.status());
        assertTrue(snap.body(), snap.body().contains("\"snapshotId\":\"" + s.snapshotIdHex() + "\""));
        assertTrue(snap.body(), snap.body().contains("\"dataVersion\":42"));
        assertTrue(snap.body(), snap.body().contains("\"codecs\":[\"CODEC_NONE\",\"CODEC_LZ4\"]"));
        assertTrue(snap.body(), snap.body().contains("\"kind\":\"CLIENT\",\"entityType\":3,\"records\":3"));
        Http report = get("/startup-report");
        assertEquals(200, report.status());
        assertTrue(report.body(), report.body().contains("\"timingsMs\":{\"load\":1,\"build\":2,\"encode\":3,\"loadAttempts\":1}"));
        assertTrue(report.body(), report.body().contains("\"validation\":{\"clean\":true,\"violations\":0,\"byRule\":{},\"quarantined\":{}}"));
        assertTrue(report.body().startsWith("{") && report.body().endsWith("}"));
        assertTrue(report.body(), report.body().contains("\"estimateBytes\":30,\"estimateAssumptions\":\"test factors\""));
        health.ready(false);
        assertEquals(503, get("/health/ready").status());
        assertEquals(404, get("/nope").status());
        assertTrue(Framing.SCHEMA_MAJOR > 0);
    }

    @Test public void metricsArePrometheusText() throws Exception {
        metrics.sessionOpened();
        metrics.transferStarted(false);
        metrics.chunkSent(100);
        metrics.transferFailed("stall");
        metrics.sessionClosed("acme \"quoted\"", "stall");
        Http m = get("/metrics");
        assertEquals(200, m.status());
        assertTrue(m.body(), m.body().contains("# TYPE rds_sessions_active gauge\nrds_sessions_active 0\n"));
        assertTrue(m.body(), m.body().contains("rds_bytes_sent_total 100\n"));
        assertTrue(m.body(), m.body().contains("rds_transfers_failed_total{reason=\"stall\"} 1\n"));
        assertTrue(m.body(), m.body().contains("rds_sessions_by_client_total{client=\"acme \\\"quoted\\\"\"} 1\n"));
        assertTrue(m.body(), m.body().contains("rds_transfers_started_total 1\n"));
    }

    @Test public void clientNameCardinalityIsCapped() {
        for (int i = 0; i < ServerMetrics.MAX_CLIENT_NAMES + 5; i++) { metrics.sessionOpened(); metrics.sessionClosed("client-" + i, "ok"); }
        assertEquals(1, metrics.sessions("client-0"));
        assertEquals(0, metrics.sessions("client-" + (ServerMetrics.MAX_CLIENT_NAMES + 2)));
        assertEquals(5, metrics.sessions(ServerMetrics.OTHER_CLIENT));
    }
}
