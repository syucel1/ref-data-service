package com.example.refdata.client;

import com.example.refdata.contract.Digests;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.Test;
import org.slf4j.MDC;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import static org.junit.Assert.*;

public class LoggingTest {
    @Test public void concurrentLoadsPreserveCallerContextAndIdentifyEachPublication() throws Exception {
        var root = (org.apache.logging.log4j.core.Logger) LogManager.getRootLogger();
        List<LogEvent> events = new CopyOnWriteArrayList<>();
        var keepAliveEnded = new java.util.concurrent.CountDownLatch(2);
        var appender = new AbstractAppender("capture-client-context", null, null, true, Property.EMPTY_ARRAY) {
            @Override public void append(LogEvent event) {
                events.add(event.toImmutable());
                if (event.getMessage().getFormattedMessage().contains("says GoAway")) keepAliveEnded.countDown();
            }
        };
        appender.start(); root.addAppender(appender);
        var before = MDC.getCopyOfContextMap();
        SnapshotFixture fixture = SnapshotFixture.sample();
        try (FakeServer server = new FakeServer()) {
            FakeServer.ConnectionScript end = session -> {
                session.send(com.example.refdata.protocol.v1.Envelope.newBuilder().setGoAway(
                    com.example.refdata.protocol.v1.GoAway.newBuilder().setReason(
                        com.example.refdata.protocol.v1.ErrorCode.ERROR_CODE_SERVER_SHUTTING_DOWN)).build());
                session.drain();
            };
            server.onConnection(new SnapshotScript(fixture).afterEnd(end));
            server.onConnection(new SnapshotScript(fixture).afterEnd(end));
            try (RefDataClient a = RefDataClient.builder().endpoints(server.endpoint()).clientName("log-a").clientInstance("a").keepAlive(true).build();
                 RefDataClient b = RefDataClient.builder().endpoints(server.endpoint()).clientName("log-b").clientInstance("b").keepAlive(true).build()) {
                MDC.put("trace", "caller-a");
                var first = a.loadAsync();
                MDC.put("trace", "caller-b");
                var second = b.loadAsync();
                first.get(10, TimeUnit.SECONDS); second.get(10, TimeUnit.SECONDS);
                assertTrue(keepAliveEnded.await(5, TimeUnit.SECONDS));
                assertEquals("caller-b", MDC.get("trace"));
                assertNull(MDC.get("rds.load"));
                for (String name : List.of("log-a", "log-b")) {
                    LogEvent event = events.stream().filter(e -> e.getMessage().getFormattedMessage().contains(" loaded:")
                        && name.equals(e.getContextData().getValue("rds.client"))).findFirst().orElseThrow();
                    assertEquals("caller-" + name.substring(4), event.getContextData().getValue("trace"));
                    assertEquals(server.endpoint(), event.getContextData().getValue("rds.endpoint"));
                    assertEquals(Digests.hex(fixture.snapshotId()), event.getContextData().getValue("rds.snapshot"));
                    assertEquals("1", event.getContextData().getValue("rds.request"));
                    assertNotNull(event.getContextData().getValue("rds.load"));
                    assertEquals("s1", event.getContextData().getValue("rds.session"));
                    LogEvent heartbeat = events.stream().filter(e -> e.getMessage().getFormattedMessage().contains("says GoAway")
                        && name.equals(e.getContextData().getValue("rds.client"))).findFirst().orElseThrow();
                    assertEquals(event.getContextData().toMap(), heartbeat.getContextData().toMap());
                }
            }
            server.assertNoFailures();
        } finally {
            root.removeAppender(appender); appender.stop();
            MDC.clear(); if (before != null) MDC.setContextMap(before);
        }
    }

    @SuppressWarnings("try")
    @Test public void contextRestoresExistingValuesAfterFailure() {
        var before = MDC.getCopyOfContextMap();
        try {
            MDC.clear(); MDC.put("trace", "original");
            assertThrows(IllegalStateException.class, () -> {
                try (LoggingContext scope = new LoggingContext(Map.of("trace", "worker"))) {
                    assertEquals("worker", MDC.get("trace"));
                    MDC.put("rds.endpoint", "temporary");
                    throw new IllegalStateException("test");
                }
            });
            assertEquals("original", MDC.get("trace"));
            assertNull(MDC.get("rds.endpoint"));
        } finally { MDC.clear(); if (before != null) MDC.setContextMap(before); }
    }
}
