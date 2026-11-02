package com.example.refdata.model.inspection;

import com.example.refdata.model.*;
import com.sun.net.httpserver.*;
import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;

/** Optional, read-only inspection of one immutable publication per request. */
public final class InspectionServer implements AutoCloseable {
    public record View(RefDataStore store, String snapshotId) {
        public View { Objects.requireNonNull(store); Objects.requireNonNull(snapshotId); }
    }
    public record Options(String host, int port, int maxPageSize) {
        public Options {
            if (host == null || host.isBlank() || port < 0 || port > 65535 || maxPageSize < 1 || maxPageSize > 500)
                throw new IllegalArgumentException("invalid inspection options");
        }
        public Options(int port) { this("127.0.0.1", port, 100); }
    }
    private final HttpServer http;
    private final ThreadPoolExecutor executor;
    private final Supplier<View> views;
    private final Options options;
    private boolean closed;

    public InspectionServer(Options options, Supplier<View> views) throws IOException {
        this.options = Objects.requireNonNull(options);
        this.views = Objects.requireNonNull(views);
        http = HttpServer.create(new InetSocketAddress(options.host(), options.port()), 16);
        executor = new ThreadPoolExecutor(2, 2, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(16), r -> {
            Thread t = new Thread(r, "refdata-inspection"); t.setDaemon(true); return t;
        }, new ThreadPoolExecutor.AbortPolicy());
        http.setExecutor(executor);
        http.createContext("/", this::handle);
        http.start();
    }
    public int port() { return http.getAddress().getPort(); }
    private void handle(HttpExchange ex) throws IOException {
        try (ex) {
            if (!ex.getRequestMethod().equals("GET")) { ex.getResponseHeaders().set("Allow", "GET"); send(ex, 405, "Read only"); return; }
            if (!ex.getRequestURI().getPath().equals("/")) { send(ex, 404, "Not found"); return; }
            String query = ex.getRequestURI().getRawQuery();
            if (query != null && query.length() > 2048) { send(ex, 400, "Query too long"); return; }
            try {
                Map<String, String> q = new HashMap<>();
                if (query != null) for (String part : query.split("&")) {
                    String[] kv = part.split("=", 2);
                    q.put(URLDecoder.decode(kv[0], StandardCharsets.UTF_8), kv.length == 1 ? "" : URLDecoder.decode(kv[1], StandardCharsets.UTF_8));
                }
                int offset = Integer.parseInt(q.getOrDefault("offset", "0"));
                int limit = Integer.parseInt(q.getOrDefault("limit", Integer.toString(Math.min(50, options.maxPageSize()))));
                if (offset < 0 || limit < 1 || limit > options.maxPageSize()) throw new IllegalArgumentException();
                EntityKind kind = EntityKind.valueOf(q.getOrDefault("cache", "COUNTRY"));
                View view = views.get(); // Exactly one publication read; no retained history or application locks.
                if (view == null) { send(ex, 503, "No snapshot published yet"); return; }
                RefDataStore s = view.store();
                StringBuilder b = new StringBuilder("<!doctype html><html lang='en'><meta charset='utf-8'><title>Cache inspection</title><body><h1>Cache inspection</h1>");
                b.append("<p>Snapshot: ").append(escape(view.snapshotId())).append(" | Data/update version: ").append(s.dataVersion()).append("</p><ul>");
                for (EntityKind k : EntityKind.values()) b.append("<li><a href='/?cache=").append(k.name()).append("'>").append(k.name()).append("</a>: ").append(s.count(k)).append("</li>");
                b.append("</ul><h2>").append(kind.name()).append("</h2><form><input type='hidden' name='cache' value='").append(kind.name())
                    .append("'><label>Primary key <input name='key' value='").append(escape(q.getOrDefault("key", "")))
                    .append("'></label><label> Page size <input name='limit' type='number' min='1' max='").append(options.maxPageSize())
                    .append("' value='").append(limit).append("'></label><button>Inspect</button></form>");
                String key = q.getOrDefault("key", "");
                if (!key.isEmpty()) {
                    Object row = lookup(s, kind, key);
                    b.append("<pre>").append(row == null ? "Not found" : escape(row.toString())).append("</pre>");
                } else {
                    List<?> rows = rows(s, kind);
                    int end = (int) Math.min((long) rows.size(), (long) offset + limit);
                    b.append("<p>Offset ").append(offset).append("; total ").append(rows.size()).append("</p>");
                    for (int i = offset; i < end; i++) b.append("<pre>").append(escape(rows.get(i).toString())).append("</pre>");
                    if (offset > 0) link(b, "Previous", kind, Math.max(0, offset - limit), limit);
                    if (end < rows.size()) link(b, "Next", kind, end, limit);
                }
                send(ex, 200, b.append("</body></html>").toString());
            } catch (IllegalArgumentException e) { send(ex, 400, "Invalid cache, primary key, offset or page size"); }
        }
    }
    private static void link(StringBuilder b, String label, EntityKind k, int offset, int limit) {
        b.append("<a href='/?cache=").append(k.name()).append("&amp;offset=").append(offset).append("&amp;limit=").append(limit).append("'>").append(label).append("</a> ");
    }
    private static List<?> rows(RefDataStore s, EntityKind k) {
        return switch (k) {
            case COUNTRY -> s.countries(); case CURRENCY -> s.currencies(); case CLIENT -> s.clients();
            case ADDRESS -> s.addresses(); case ACCOUNT -> s.accounts(); case SETTLEMENT_INSTRUCTION -> s.settlementInstructions();
        };
    }
    private static Object lookup(RefDataStore s, EntityKind k, String key) {
        return switch (k) {
            case COUNTRY -> s.country(key); case CURRENCY -> s.currency(key); case CLIENT -> s.client(Long.parseLong(key));
            case ADDRESS -> s.address(Long.parseLong(key)); case ACCOUNT -> s.account(Long.parseLong(key));
            case SETTLEMENT_INSTRUCTION -> s.settlementInstruction(Long.parseLong(key));
        };
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
    @Override public synchronized void close() {
        if (closed) return;
        closed = true;
        http.stop(0);
        executor.shutdownNow();
        try { executor.awaitTermination(2, TimeUnit.SECONDS); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
