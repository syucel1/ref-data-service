package com.example.refdata.server.admin;

import com.example.refdata.contract.ChangeCodec;
import com.example.refdata.model.ChangeBatch;
import com.example.refdata.model.validation.ValidationException;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.SQLException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The service-owned transactional write API for live updates ({@code rds.admin.port}, loopback only).
 * <p>{@code POST /changes} with {@code Authorization: Bearer <rds.admin.token>} and a {@link ChangeCodec} body:
 * the batch is validated against the served store, written to the database with its outbox row in one transaction,
 * then published by the {@link OutboxRelay}. Responses carry no body:
 * <ul>
 *   <li>204 committed and published</li>
 *   <li>400 the body is not a well-formed batch</li>
 *   <li>401 missing or wrong token; 404 other path; 405 not POST; 413 body over {@link ChangeCodec#MAX_BATCH_BYTES}</li>
 *   <li>409 the batch does not apply: version gap, INSERT of an existing key, UPDATE/DELETE of a missing one, or the
 *       resulting data breaks a validation rule</li>
 *   <li>503 the database rejected the write (rolled back) or the batch could not be published</li>
 * </ul>
 * One worker handles requests one at a time (a bounded queue of 16 rejects overload).
 */
public final class AdminServer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(AdminServer.class);
    public static final int MIN_TOKEN_LENGTH = 16;
    static final String PATH = "/changes";
    private static final long RELAY_INTERVAL_MS = 100;
    /** Frame bytes around an update_batch body (length prefix, envelope tag and length), with room to spare. */
    private static final int ENVELOPE_OVERHEAD = 32;

    private final ChangePublisher publisher;
    private final ChangeWriter writer;
    private final OutboxRelay relay;
    private final int maxFrameBytes;
    private final byte[] expectedAuthorization;
    private final HttpServer http;
    private final ThreadPoolExecutor worker = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(16),
            r -> daemon(r, "rds-admin"), new ThreadPoolExecutor.AbortPolicy());
    private final ScheduledExecutorService relayTimer = Executors.newSingleThreadScheduledExecutor(r -> daemon(r, "rds-outbox"));

    /**
     * Creates the outbox table if needed, binds {@code 127.0.0.1:port} (0 = any free port) and starts relaying.
     * @param maxFrameBytes the server's frame limit; a batch that would not fit one update frame is refused
     */
    public AdminServer(ChangePublisher publisher, ConnectionFactory connections, int port, String token, int maxFrameBytes) throws IOException, SQLException {
        if (token == null || token.length() < MIN_TOKEN_LENGTH) throw new IllegalArgumentException("admin token must contain at least " + MIN_TOKEN_LENGTH + " characters");
        this.publisher = publisher;
        this.writer = new ChangeWriter(connections);
        this.relay = new OutboxRelay(connections, publisher);
        this.maxFrameBytes = maxFrameBytes;
        this.expectedAuthorization = ("Bearer " + token).getBytes(StandardCharsets.UTF_8);
        writer.ensureOutboxTable();
        http = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 16);
        http.setExecutor(worker);
        http.createContext(PATH, this::handle);
        http.start();
        relayTimer.scheduleWithFixedDelay(relay::drainQuietly, 0, RELAY_INTERVAL_MS, TimeUnit.MILLISECONDS);
        log.info("admin change endpoint listening on {}", http.getAddress());
    }

    /** The bound port. */
    public int port() { return http.getAddress().getPort(); }

    /**
     * Validates, commits and publishes one batch (what {@code POST /changes} does).
     * @throws IllegalArgumentException or {@link ValidationException} when the batch does not apply to the served store
     * @throws SQLException when the database rejects the write (nothing is committed)
     * @throws IOException  when the batch is too large for one frame, or publication fails after the commit (the
     *                      relay retries it)
     */
    public void commit(ChangeBatch batch) throws SQLException, IOException {
        synchronized (relay) {
            relay.drain();                       // publish anything pending first, so the batch is checked against the latest store
            publisher.preview(batch);            // validates the complete resulting graph before touching the database
            byte[] payload = ChangeCodec.encode(batch);
            if (payload.length + ENVELOPE_OVERHEAD > maxFrameBytes) throw new IOException("batch of " + payload.length + " bytes exceeds the frame limit " + maxFrameBytes);
            writer.write(batch, payload);
            relay.drain();
        }
    }

    private void handle(HttpExchange ex) throws IOException {
        try (ex) {
            if (!authorized(ex)) { ex.sendResponseHeaders(401, -1); return; }
            if (!ex.getRequestURI().getPath().equals(PATH)) { ex.sendResponseHeaders(404, -1); return; }
            if (!ex.getRequestMethod().equals("POST")) { ex.getResponseHeaders().set("Allow", "POST"); ex.sendResponseHeaders(405, -1); return; }
            byte[] body = ex.getRequestBody().readNBytes(ChangeCodec.MAX_BATCH_BYTES + 1);
            if (body.length > ChangeCodec.MAX_BATCH_BYTES) { ex.sendResponseHeaders(413, -1); return; }
            ex.sendResponseHeaders(process(body), -1);
        }
    }

    /** Runs one request and maps the outcome to a status code. */
    private int process(byte[] body) {
        ChangeBatch batch;
        try {
            batch = ChangeCodec.decode(body);
        } catch (IOException e) {
            log.warn("admin request rejected: malformed batch: {}", e.getMessage());
            return 400;
        }
        try {
            commit(batch);
            log.info("committed and published batch {} -> {} ({} changes)", batch.baseVersion(), batch.version(), batch.changes().size());
            return 204;
        } catch (IllegalArgumentException | ValidationException e) {
            log.warn("admin batch {} -> {} rejected: {}", batch.baseVersion(), batch.version(), e.getMessage());
            return 409;
        } catch (SQLException | IOException | RuntimeException e) {
            log.error("admin batch {} -> {} failed", batch.baseVersion(), batch.version(), e);
            return 503;
        }
    }

    /** Constant-time comparison of the Authorization header. */
    private boolean authorized(HttpExchange ex) {
        String header = ex.getRequestHeaders().getFirst("Authorization");
        return header != null && MessageDigest.isEqual(expectedAuthorization, header.getBytes(StandardCharsets.UTF_8));
    }

    private static Thread daemon(Runnable r, String name) {
        Thread t = new Thread(r, name);
        t.setDaemon(true);
        return t;
    }

    /** Stops the endpoint and the relay. */
    @Override public void close() {
        http.stop(0);
        worker.shutdownNow();
        relayTimer.shutdownNow();
        try {
            worker.awaitTermination(5, TimeUnit.SECONDS);
            relayTimer.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
