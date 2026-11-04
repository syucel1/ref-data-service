package com.example.refdata.model.inspection;

import com.example.refdata.model.EntityKind;
import com.example.refdata.model.RefDataStore;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Optional, read-only HTML inspection of the cache. Every request reads exactly one immutable publication (a
 * {@link View}) from the supplier, so a page is always consistent; no history is retained and no application lock is
 * taken. Two daemon workers and a 16-request queue bound its cost; overload is rejected rather than queued further.
 */
public final class InspectionServer implements AutoCloseable {
    /** Default page size when the request names none (capped by {@link Options#maxPageSize()}). */
    static final int DEFAULT_PAGE_SIZE = 50;
    private static final int MAX_QUERY_LENGTH = 2048;
    private static final int WORKERS = 2;
    private static final int QUEUE_AND_BACKLOG = 16;

    /** One publication: the store and the id of the snapshot it came from. */
    public record View(RefDataStore store, String snapshotId) {
        public View { Objects.requireNonNull(store); Objects.requireNonNull(snapshotId); }
    }

    /** Where to listen and the largest page a request may ask for (1..500). */
    public record Options(String host, int port, int maxPageSize) {
        public Options {
            if (host == null || host.isBlank() || port < 0 || port > 65535 || maxPageSize < 1 || maxPageSize > 500)
                throw new IllegalArgumentException("invalid inspection options");
        }
        /** Loopback, the given port, at most 100 rows per page. */
        public Options(int port) { this("127.0.0.1", port, 100); }
    }

    /** A parsed, validated request. {@code key} empty means "page through the rows". */
    private record PageRequest(EntityKind kind, String key, int offset, int limit) {}

    private final HttpServer http;
    private final ThreadPoolExecutor executor;
    private final Supplier<View> views;
    private final Options options;
    private boolean closed;

    /** Starts listening at once. */
    public InspectionServer(Options options, Supplier<View> views) throws IOException {
        this.options = Objects.requireNonNull(options);
        this.views = Objects.requireNonNull(views);
        http = HttpServer.create(new InetSocketAddress(options.host(), options.port()), QUEUE_AND_BACKLOG);
        executor = new ThreadPoolExecutor(WORKERS, WORKERS, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(QUEUE_AND_BACKLOG), r -> {
            Thread t = new Thread(r, "refdata-inspection");
            t.setDaemon(true);
            return t;
        }, new ThreadPoolExecutor.AbortPolicy());
        http.setExecutor(executor);
        http.createContext("/", this::handle);
        http.start();
    }

    /** The bound port (useful when configured as 0). */
    public int port() { return http.getAddress().getPort(); }

    private void handle(HttpExchange ex) throws IOException {
        try (ex) {
            if (!ex.getRequestMethod().equals("GET")) { ex.getResponseHeaders().set("Allow", "GET"); send(ex, 405, "Read only"); return; }
            if (!ex.getRequestURI().getPath().equals("/")) { send(ex, 404, "Not found"); return; }
            String query = ex.getRequestURI().getRawQuery();
            if (query != null && query.length() > MAX_QUERY_LENGTH) { send(ex, 400, "Query too long"); return; }
            PageRequest request;
            try {
                request = parse(query);
            } catch (IllegalArgumentException e) {
                send(ex, 400, "Invalid cache, primary key, offset or page size");
                return;
            }
            View view = views.get();           // exactly one publication per request
            if (view == null) { send(ex, 503, "No snapshot published yet"); return; }
            try {
                send(ex, 200, render(view, request));
            } catch (NumberFormatException e) {
                send(ex, 400, "Invalid cache, primary key, offset or page size");
            }
        }
    }

    private PageRequest parse(String rawQuery) {
        Map<String, String> q = new HashMap<>();
        if (rawQuery != null) for (String part : rawQuery.split("&")) {
            String[] kv = part.split("=", 2);
            q.put(URLDecoder.decode(kv[0], StandardCharsets.UTF_8), kv.length == 1 ? "" : URLDecoder.decode(kv[1], StandardCharsets.UTF_8));
        }
        int offset = Integer.parseInt(q.getOrDefault("offset", "0"));
        int limit = Integer.parseInt(q.getOrDefault("limit", Integer.toString(Math.min(DEFAULT_PAGE_SIZE, options.maxPageSize()))));
        if (offset < 0 || limit < 1 || limit > options.maxPageSize()) throw new IllegalArgumentException("offset or limit out of range");
        EntityKind kind = EntityKind.valueOf(q.getOrDefault("cache", EntityKind.COUNTRY.name()));
        return new PageRequest(kind, q.getOrDefault("key", ""), offset, limit);
    }

    private String render(View view, PageRequest r) {
        RefDataStore s = view.store();
        StringBuilder b = new StringBuilder("<!doctype html><html lang='en'><meta charset='utf-8'><title>Cache inspection</title><body><h1>Cache inspection</h1>");
        b.append("<p>Snapshot: ").append(escape(view.snapshotId())).append(" | Data/update version: ").append(s.dataVersion()).append("</p><ul>");
        for (EntityKind k : EntityKind.values()) {
            b.append("<li><a href='/?cache=").append(k.name()).append("'>").append(k.name()).append("</a>: ").append(s.count(k)).append("</li>");
        }
        b.append("</ul><h2>").append(r.kind().name()).append("</h2><form><input type='hidden' name='cache' value='").append(r.kind().name())
                .append("'><label>Primary key <input name='key' value='").append(escape(r.key()))
                .append("'></label><label> Page size <input name='limit' type='number' min='1' max='").append(options.maxPageSize())
                .append("' value='").append(r.limit()).append("'></label><button>Inspect</button></form>");
        if (!r.key().isEmpty()) {
            Object row = s.findByKey(r.kind(), r.key());
            b.append("<pre>").append(row == null ? "Not found" : escape(row.toString())).append("</pre>");
        } else {
            List<?> rows = s.rows(r.kind());
            int end = (int) Math.min(rows.size(), (long) r.offset() + r.limit());
            b.append("<p>Offset ").append(r.offset()).append("; total ").append(rows.size()).append("</p>");
            for (int i = r.offset(); i < end; i++) b.append("<pre>").append(escape(rows.get(i).toString())).append("</pre>");
            if (r.offset() > 0) link(b, "Previous", r.kind(), Math.max(0, r.offset() - r.limit()), r.limit());
            if (end < rows.size()) link(b, "Next", r.kind(), end, r.limit());
        }
        return b.append("</body></html>").toString();
    }

    private static void link(StringBuilder b, String label, EntityKind k, int offset, int limit) {
        b.append("<a href='/?cache=").append(k.name()).append("&amp;offset=").append(offset).append("&amp;limit=").append(limit).append("'>").append(label).append("</a> ");
    }

    private static String escape(String s) {
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;").replace("'", "&#39;");
    }

    private static void send(HttpExchange ex, int status, String body) throws IOException {
        ex.getResponseHeaders().set("Content-Type", "text/html; charset=utf-8");
        ex.getResponseHeaders().set("Cache-Control", "no-store");
        ex.getResponseHeaders().set("Content-Security-Policy", "default-src 'none'; form-action 'self'; frame-ancestors 'none'");
        ex.getResponseHeaders().set("X-Content-Type-Options", "nosniff");
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.sendResponseHeaders(status, bytes.length);
        ex.getResponseBody().write(bytes);
    }

    /** Stops listening and stops the workers. Idempotent. */
    @Override public synchronized void close() {
        if (closed) return;
        closed = true;
        http.stop(0);
        executor.shutdownNow();
        try { executor.awaitTermination(2, TimeUnit.SECONDS); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
