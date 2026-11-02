package com.example.refdata.client;

import com.example.refdata.contract.Framing;
import com.example.refdata.protocol.v1.Envelope;
import com.example.refdata.protocol.v1.Heartbeat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.Closeable;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.time.Duration;

/**
 * Keeps a connection open after the load: one thread sends a heartbeat every interval on a fixed schedule (the
 * server counts only inbound frames for its idle check), another reads and closes quietly on GoAway, an error
 * reply, a dead peer ({@link Framing#HEARTBEAT_MISSES} silent intervals) or {@link #close()} (design row C-12;
 * this library does not reconnect a kept-alive connection).
 */
final class KeepAliveSession implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(KeepAliveSession.class);

    private final ServerConnection connection;
    private final Duration interval;
    private final Thread reader;
    private final Thread sender;
    private volatile boolean closed;

    KeepAliveSession(ServerConnection connection, Duration interval) {
        this.connection = connection;
        this.interval = interval;
        this.sender = daemon(this::sendLoop, "refdata-client-keepalive-send");
        this.reader = daemon(this::readLoop, "refdata-client-keepalive-read");
        sender.start();
        reader.start();
    }

    @SuppressWarnings("try") // The resource restores MDC on worker termination.
    private static Thread daemon(Runnable body, String name) {
        var context = MDC.getCopyOfContextMap();
        Thread t = new Thread(() -> {
            try (LoggingContext scope = new LoggingContext(context)) { body.run(); }
        }, name);
        t.setDaemon(true);
        return t;
    }

    private void sendLoop() {
        try {
            while (!closed) {
                connection.send(Envelope.newBuilder().setHeartbeat(Heartbeat.newBuilder().setSentAtEpochMs(System.currentTimeMillis())).build());
                Thread.sleep(interval);
            }
        } catch (IOException e) {
            if (!closed) log.info("kept-alive connection to {} ended while sending a heartbeat: {}", connection.endpoint(), e.toString());
            stop();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void readLoop() {
        try {
            connection.readTimeout(interval.multipliedBy(Framing.HEARTBEAT_MISSES));
            while (!closed) {
                Envelope frame = connection.read();
                switch (frame.getBodyCase()) {
                    case HEARTBEAT -> { }
                    case GO_AWAY -> {
                        log.info("server {} says GoAway ({}); closing kept-alive connection", connection.endpoint(), frame.getGoAway().getReason());
                        return;
                    }
                    case ERROR -> {
                        log.warn("server {} sent {} on the kept-alive connection; closing", connection.endpoint(), frame.getError().getCode());
                        return;
                    }
                    default -> log.debug("ignoring {} on the kept-alive connection", frame.getBodyCase());
                }
            }
        } catch (SocketTimeoutException e) {
            log.warn("nothing from {} for {} heartbeat intervals; closing kept-alive connection", connection.endpoint(), Framing.HEARTBEAT_MISSES);
        } catch (IOException e) {
            if (!closed) log.info("kept-alive connection to {} ended: {}", connection.endpoint(), e.toString());
        } finally {
            stop();
        }
    }

    private void stop() {
        closed = true;
        connection.close();
        sender.interrupt();
    }

    @Override public void close() { stop(); }
}
