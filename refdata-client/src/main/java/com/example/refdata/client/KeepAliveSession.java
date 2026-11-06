package com.example.refdata.client;

import com.example.refdata.contract.ChangeCodec;
import com.example.refdata.contract.Framing;
import com.example.refdata.model.ChangeBatch;
import com.example.refdata.protocol.v1.Envelope;
import com.example.refdata.protocol.v1.Heartbeat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.Closeable;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.function.Consumer;

/**
 * Keeps a connection open after the load: one thread sends a heartbeat every interval on a fixed schedule (the
 * server counts only inbound frames for its idle check), another reads and closes quietly on GoAway, an error
 * reply, a dead peer ({@link Framing#HEARTBEAT_MISSES} silent intervals) or {@link #close()} (design row C-12).
 * On a live-update subscription the reader also hands every {@code update_batch} to the update listener; the
 * subscription in {@link RefDataClient} reloads a snapshot once this session has closed.
 */
final class KeepAliveSession implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(KeepAliveSession.class);

    private final ServerConnection connection;
    private final Duration interval;
    private final Thread reader;
    private final Thread sender;
    private final Consumer<ChangeBatch> updates;
    private volatile boolean closed;
    private volatile boolean updateFailed;

    KeepAliveSession(ServerConnection connection, Duration interval) {
        this(connection, interval, b -> { });
    }

    /** @param updates receives each live update batch, on the reader thread; a failure closes the connection */
    KeepAliveSession(ServerConnection connection, Duration interval, Consumer<ChangeBatch> updates) {
        this.updates = updates;
        this.connection = connection;
        this.interval = interval;
        this.sender = daemon(this::sendLoop, "refdata-client-keepalive-send");
        this.reader = daemon(this::readLoop, "refdata-client-keepalive-read");
        sender.start();
        reader.start();
    }

    /** True once the connection has ended (for any reason) or {@link #close()} was called. */
    boolean closed() { return closed; }

    /** True when the connection was closed because a live update could not be decoded or applied. */
    boolean updateFailed() { return updateFailed; }

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
                    case UPDATE_BATCH -> updates.accept(ChangeCodec.decode(frame.getUpdateBatch().toByteArray()));
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
        } catch (RuntimeException e) {
            updateFailed = true;
            log.error("live update failed; closing for resynchronisation", e);
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
