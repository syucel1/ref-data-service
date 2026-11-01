package com.example.refdata.server.health;

import com.example.refdata.contract.Digests;
import com.example.refdata.model.EntityKind;
import com.example.refdata.server.snapshot.Snapshot;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The operations HTTP endpoint on {@code rds.http.port} (JDK built-in server, one small thread pool):
 * {@code GET /health/live}, {@code /health/ready} (503 until the snapshot is published), {@code /metrics}
 * (Prometheus text), {@code /snapshot} (JSON summary) and {@code /startup-report} (JSON).
 */
public final class HealthServer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(HealthServer.class);

    private final ServerMetrics metrics;
    private final AtomicReference<Snapshot> snapshot = new AtomicReference<>();
    private final AtomicReference<StartupReport> report = new AtomicReference<>();
    private final AtomicReference<String> phase = new AtomicReference<>("starting");
    private volatile boolean ready;
    private HttpServer http;

    public HealthServer(ServerMetrics metrics) {
        this.metrics = metrics;
    }

    /** Binds the port (0 = any free port) and starts serving; readiness is DOWN until {@link #published}. */
    public synchronized void start(String bindAddress, int port) throws IOException {
        if (http != null) throw new IllegalStateException("already started");
        try {
            http = HttpServer.create(new InetSocketAddress(bindAddress, port), 16);
        } catch (IOException e) {
            throw new IOException("cannot bind HTTP port " + port + " on " + bindAddress + ": " + e.getMessage(), e);
        }
        AtomicInteger n = new AtomicInteger();
        http.setExecutor(Executors.newFixedThreadPool(2, r -> { Thread t = new Thread(r, "rds-http-" + n.incrementAndGet()); t.setDaemon(true); return t; }));
        http.createContext("/health/live", text(() -> new Response(200, "UP")));
        http.createContext("/health/ready", text(() -> ready ? new Response(200, "READY") : new Response(503, "NOT READY: " + phase.get())));
        http.createContext("/metrics", text(() -> new Response(200, metrics.render())));
        http.createContext("/snapshot", json(() -> { Snapshot s = snapshot.get(); return s == null ? new Response(503, "{\"status\":\"" + phase.get() + "\"}") : new Response(200, snapshotJson(s)); }));
        http.createContext("/startup-report", json(() -> { StartupReport r = report.get(); return r == null ? new Response(503, "{\"status\":\"" + phase.get() + "\"}") : new Response(200, r.toJson()); }));
        http.createContext("/", text(() -> new Response(404, "not found")));
        http.start();
        log.info("HTTP endpoint listening on {}", http.getAddress());
    }

    /** The bound port. */
    public synchronized int port() {
        if (http == null) throw new IllegalStateException("not started");
        return http.getAddress().getPort();
    }

    /** Startup phase shown while not ready (for example "loading", "encoding"). */
    public void phase(String p) { phase.set(p); }

    /** Makes the snapshot visible on /snapshot and /startup-report. */
    public void published(Snapshot s, StartupReport r) {
        snapshot.set(s);
        report.set(r);
    }

    /** Flips readiness (true once the TCP port is open; false again during shutdown). */
    public void ready(boolean up) { ready = up; }

    public boolean isReady() { return ready; }

    @Override public synchronized void close() {
        if (http == null) return;
        http.stop(0);
        ((java.util.concurrent.ExecutorService) http.getExecutor()).shutdownNow();
        try { ((java.util.concurrent.ExecutorService) http.getExecutor()).awaitTermination(2, TimeUnit.SECONDS); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        http = null;
        log.info("HTTP endpoint stopped");
    }

    static String snapshotJson(Snapshot s) {
        Json j = new Json().obj().str("snapshotId", s.snapshotIdHex()).num("dataVersion", s.dataVersion())
                .num("schemaMajor", s.info().getSchemaMajor()).num("schemaMinor", s.info().getSchemaMinor())
                .num("builtAtEpochMs", s.info().getBuiltAtEpochMs()).str("source", s.info().getSource())
                .str("contentSha256", Digests.hex(s.contentSha256())).num("totalChunks", s.chunkCount())
                .num("totalRecords", s.totalRecords()).num("totalUncompressedBytes", s.totalUncompressedBytes())
                .num("largestFrameBytes", s.largestFrameBytes());
        j.key("codecs").arr();
        s.codecs().forEach(c -> j.str(c.name()));
        j.end().key("entities").arr();
        for (EntityKind k : EntityKind.values()) {
            j.obj().str("kind", k.name()).num("entityType", k.wireCode()).num("records", s.manifest(k).getRecordCount())
                    .num("firstSeq", s.manifest(k).getFirstSeq()).num("chunks", s.manifest(k).getChunkCount())
                    .num("uncompressedBytes", s.manifest(k).getUncompressedBytes()).str("sha256", Digests.hex(s.entityHash(k))).end();
        }
        return j.end().end().toString();
    }

    private record Response(int status, String body) {
    }

    private interface Producer { Response get(); }

    private static HttpHandler text(Producer p) { return handler(p, "text/plain; charset=utf-8"); }
    private static HttpHandler json(Producer p) { return handler(p, "application/json"); }

    private static HttpHandler handler(Producer p, String contentType) {
        return (HttpExchange ex) -> {
            try (ex) {
                if (!"GET".equals(ex.getRequestMethod())) { ex.sendResponseHeaders(405, -1); return; }
                Response r;
                try { r = p.get(); }
                catch (RuntimeException e) { log.error("health handler failed", e); r = new Response(500, "internal error"); }
                byte[] body = r.body().getBytes(StandardCharsets.UTF_8);
                ex.getResponseHeaders().set("Content-Type", contentType);
                ex.sendResponseHeaders(r.status(), body.length);
                try (OutputStream out = ex.getResponseBody()) { out.write(body); }
            }
        };
    }
}
