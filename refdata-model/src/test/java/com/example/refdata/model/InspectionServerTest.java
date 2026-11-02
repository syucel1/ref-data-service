package com.example.refdata.model;

import com.example.refdata.model.inspection.InspectionServer;
import org.junit.Test;
import java.net.*;
import java.net.http.*;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.junit.Assert.*;

public class InspectionServerTest {
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    private HttpResponse<String> get(int port, String query) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/" + query))
                .timeout(Duration.ofSeconds(5)).build(), HttpResponse.BodyHandlers.ofString());
    }
    private static InspectionServer.View view(long version) {
        return new InspectionServer.View(Fixtures.validSet().dataVersion(version).build().store(), "snapshot-" + version);
    }
    @Test public void pagesLookupsLimitsAndShutdown() throws Exception {
        InspectionServer server = new InspectionServer(new InspectionServer.Options(0), () -> view(7));
        int port = server.port();
        try {
            String first = get(port, "?cache=CLIENT&limit=2").body();
            assertTrue(first.contains("snapshot-7"));
            assertTrue(first.contains("CLIENT</a>: 4"));
            assertTrue(first.contains("Client[id=1,"));
            assertTrue(first.contains("Client[id=2,"));
            assertFalse(first.contains("Client[id=3,"));
            String last = get(port, "?cache=CLIENT&offset=2&limit=2").body();
            assertTrue(last.contains("Client[id=3,"));
            assertTrue(last.contains("Client[id=10,"));
            assertFalse(last.contains(">Next</a>"));
            assertTrue(get(port, "?cache=COUNTRY&key=GB").body().contains("United Kingdom"));
            assertTrue(get(port, "?cache=CLIENT&key=999").body().contains("Not found"));
            for (String q : new String[]{"?limit=101", "?limit=0", "?offset=-1", "?cache=BAD", "?cache=CLIENT&key=x", "?offset=9999999999999"})
                assertEquals(q, 400, get(port, q).statusCode());
            assertEquals(200, get(port, "?offset=2147483647&limit=100").statusCode());
            assertEquals(405, client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port))
                .POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString()).statusCode());
        } finally { server.close(); server.close(); }
        assertThrows(java.io.IOException.class, () -> get(port, ""));
    }
    @Test public void publicationDoesNotWaitForInspectionAndEachResponseUsesOneView() throws Exception {
        AtomicReference<InspectionServer.View> current = new AtomicReference<>(view(7));
        CountDownLatch captured = new CountDownLatch(1), release = new CountDownLatch(1);
        AtomicInteger reads = new AtomicInteger();
        try (InspectionServer server = new InspectionServer(new InspectionServer.Options(0), () -> {
            reads.incrementAndGet();
            var v = current.get();
            captured.countDown();
            try { if (!release.await(5, TimeUnit.SECONDS)) throw new AssertionError("timeout"); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            return v;
        })) {
            var response = client.sendAsync(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.port())).build(), HttpResponse.BodyHandlers.ofString());
            try {
                assertTrue(captured.await(2, TimeUnit.SECONDS));
                var next = view(8);
                CompletableFuture.runAsync(() -> current.set(next)).get(1, TimeUnit.SECONDS);
                assertEquals(8, current.get().store().dataVersion());
            } finally { release.countDown(); }
            String body = response.get(3, TimeUnit.SECONDS).body();
            assertTrue(body.contains("snapshot-7"));
            assertTrue(body.contains("version: 7"));
            assertEquals(1, reads.get());
            assertTrue(get(server.port(), "").body().contains("snapshot-8"));
        }
    }
    @Test public void unavailableAndEscapedValues() throws Exception {
        AtomicReference<InspectionServer.View> v = new AtomicReference<>();
        try (InspectionServer server = new InspectionServer(new InspectionServer.Options(0), v::get)) {
            assertEquals(503, get(server.port(), "").statusCode());
            v.set(new InspectionServer.View(Fixtures.validSet().build().store(), "<script>'"));
            String body = get(server.port(), "?key=%27%3E%3Cscript%3E").body();
            assertFalse(body.contains("<script>"));
            assertTrue(body.contains("&lt;script&gt;"));
        }
    }
    @Test public void boundedWorkersAndShutdownInterruptActiveInspection() throws Exception {
        CountDownLatch active = new CountDownLatch(2);
        CountDownLatch interrupted = new CountDownLatch(2);
        CountDownLatch never = new CountDownLatch(1);
        AtomicInteger running = new AtomicInteger(), maximum = new AtomicInteger();
        var v = view(7);
        InspectionServer server = new InspectionServer(new InspectionServer.Options(0), () -> {
            maximum.accumulateAndGet(running.incrementAndGet(), Math::max);
            active.countDown();
            try { never.await(); }
            catch (InterruptedException e) { interrupted.countDown(); Thread.currentThread().interrupt(); }
            finally { running.decrementAndGet(); }
            return v;
        });
        java.util.List<CompletableFuture<HttpResponse<String>>> requests = new java.util.ArrayList<>();
        try {
            for (int i = 0; i < 24; i++) requests.add(client.sendAsync(HttpRequest.newBuilder(
                URI.create("http://127.0.0.1:" + server.port())).timeout(Duration.ofSeconds(5)).build(), HttpResponse.BodyHandlers.ofString()));
            assertTrue(active.await(3, TimeUnit.SECONDS));
            CompletableFuture.runAsync(server::close).get(3, TimeUnit.SECONDS);
            assertTrue(interrupted.await(1, TimeUnit.SECONDS));
            assertEquals(2, maximum.get());
        } finally { server.close(); never.countDown(); requests.forEach(r -> r.cancel(true)); }
    }
}
