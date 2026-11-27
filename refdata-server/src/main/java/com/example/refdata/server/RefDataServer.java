package com.example.refdata.server;

import com.example.refdata.contract.ChangeCodec;
import com.example.refdata.model.ChangeBatch;
import com.example.refdata.model.EntityKind;
import com.example.refdata.model.RefDataSource;
import com.example.refdata.model.RefDataStore;
import com.example.refdata.model.RefDataStoreBuilder;
import com.example.refdata.model.inspection.InspectionServer;
import com.example.refdata.model.validation.ValidationException;
import com.example.refdata.model.validation.ValidationReport;
import com.example.refdata.protocol.v1.Envelope;
import com.example.refdata.server.admin.AdminServer;
import com.example.refdata.server.admin.ChangePublisher;
import com.example.refdata.server.admin.ConnectionFactory;
import com.example.refdata.server.admin.ReloadRejectedException;
import com.example.refdata.server.admin.ReloadResult;
import com.example.refdata.server.health.HealthServer;
import com.example.refdata.server.health.ServerMetrics;
import com.example.refdata.server.health.StartupReport;
import com.example.refdata.server.net.RefDataTcpServer;
import com.example.refdata.server.snapshot.Snapshot;
import com.example.refdata.server.snapshot.SnapshotEncoder;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The whole service in one object, for the launcher and for in-process tests: the startup sequence of DESIGN.md 7.1
 * (load, validate, encode, report, open the ports) and the planned shutdown (GoAway, drain, close).
 * {@link #start} runs it against a {@link RefDataSource}; {@link #startWithStore} skips the database.
 */
public final class RefDataServer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(RefDataServer.class);
    /** Reported in HelloAck.server_version. */
    public static final String VERSION = "refdata-server 0.1.0";
    /** Frame bytes around an update_batch body (length prefix, envelope tag and length), with room to spare. */
    private static final int UPDATE_FRAME_OVERHEAD = 32;

    private final ServerConfig config;
    private final ServerMetrics metrics = new ServerMetrics();
    private final HealthServer health = new HealthServer(metrics);
    private final String serverId;
    private final CountDownLatch terminated = new CountDownLatch(1);
    private final SnapshotEncoder encoder;
    private InspectionServer inspection;
    private AdminServer admin;
    private volatile Snapshot snapshot;
    /** The published store and snapshot id, replaced as one reference so readers always see a matching pair. */
    private volatile InspectionServer.View published;
    private StartupReport report;
    private RefDataTcpServer tcp;
    private boolean closed;
    /** Where {@link #reload} reads from; null when started from a built store. */
    private RefDataSource source;
    /** One reload at a time; held while loading, so it is never the server monitor that readers and publishers need. */
    private final ReentrantLock reloadLock = new ReentrantLock();

    private RefDataServer(ServerConfig config) {
        this.config = config;
        this.encoder = new SnapshotEncoder(config.chunkTargetBytes, config.frameMaxBytes, config.codecs);
        String host;
        try { host = InetAddress.getLocalHost().getHostName(); } catch (IOException e) { host = "localhost"; }
        this.serverId = host + ":" + config.tcpPort;
    }

    /**
     * Full startup: HTTP endpoint (not ready), database pre-flight and load with retries, validation, encoding, the
     * startup report, then the TCP port (readiness UP). Throws {@link StoreLoader.StartupException} (or an
     * {@link IllegalStateException} for a port that cannot be bound) with nothing left running.
     */
    public static RefDataServer start(ServerConfig config, RefDataSource source) throws InterruptedException {
        RefDataServer s = new RefDataServer(config);
        s.source = source;
        try {
            s.openHealth();
            StoreLoader.Loaded loaded = new StoreLoader(config, s.health::phase).load(source);
            s.publish(loaded.store(), loaded.validation(), loaded.preflight().sourceDescription(), loaded.preflight().rowCounts(), loaded.loadedRows(),
                    loaded.loadMs(), loaded.buildMs(), loaded.loadAttempts(), loaded.heapEstimateBytes(), loaded.heapAssumptions());
            return s;
        } catch (RuntimeException | InterruptedException e) {
            s.close();
            throw e;
        }
    }

    /** Startup from an already built store (no database): encode, report, open the ports. */
    public static RefDataServer startWithStore(ServerConfig config, RefDataStore store, String sourceDescription) throws InterruptedException {
        RefDataServer s = new RefDataServer(config);
        try {
            s.openHealth();
            Map<EntityKind, Long> counts = new EnumMap<>(EntityKind.class);
            for (EntityKind k : EntityKind.values()) counts.put(k, (long) store.count(k));
            s.publish(store, new ValidationReport(), sourceDescription, counts, counts, 0, 0, 0, 0, "no pre-flight (started from a built store)");
            return s;
        } catch (RuntimeException | InterruptedException e) {
            s.close();
            throw e;
        }
    }

    private void openHealth() {
        log.info("effective configuration:\n{}", config);
        try {
            health.start(config.bindAddress, config.httpPort);
        } catch (IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    private void publish(RefDataStore store, ValidationReport validation, String source, Map<EntityKind, Long> preflightRows, Map<EntityKind, Long> loadedRows,
                         long loadMs, long buildMs, int loadAttempts, long heapEstimateBytes, String heapAssumptions) throws InterruptedException {
        health.phase("encoding");
        long t0 = System.nanoTime();
        snapshot = encoder.encode(store, source, System.currentTimeMillis());
        long encodeMs = (System.nanoTime() - t0) / 1_000_000;
        Runtime rt = Runtime.getRuntime();
        report = new StartupReport(source, preflightRows, loadedRows, store.dataVersion(), loadMs, buildMs, encodeMs, validation,
                rt.totalMemory() - rt.freeMemory(), rt.maxMemory(), heapEstimateBytes, heapAssumptions, loadAttempts, snapshot);
        log.info(report.toText());
        health.published(snapshot, report);
        published = new InspectionServer.View(store, snapshot.snapshotIdHex());
        if (config.inspectionEnabled) {
            try { inspection = new InspectionServer(config.inspectionOptions, () -> published); }
            catch (IOException e) { throw new IllegalStateException("cannot start cache inspection", e); }
        }
        health.phase("opening port");
        tcp = new RefDataTcpServer(config, snapshot, metrics, serverId, VERSION);
        tcp.onDegraded(() -> { health.phase("degraded: a stored snapshot frame failed its CRC check, restart needed"); health.ready(false); });
        tcp.start();
        if (config.adminPort >= 0) {
            try {
                admin = new AdminServer(changePublisher(), ConnectionFactory.driverManager(config.dbUrl, config.dbUser, config.dbPassword()),
                        config.adminPort, config.adminToken, config.frameMaxBytes);
            } catch (IOException | java.sql.SQLException e) {
                throw new IllegalStateException("cannot start admin change endpoint", e);
            }
        }
        health.ready(true);
        log.info("ready: snapshot {} on TCP port {}, health on HTTP port {}", snapshot.snapshotIdHex(), tcp.port(), health.port());
    }

    /**
     * Publishes a transaction that is already committed to the database: builds and validates the next store, encodes
     * the next snapshot (new connections receive it) and sends the batch to every live session.
     * @throws IOException when the batch does not fit one frame
     * @throws IllegalArgumentException or {@link com.example.refdata.model.validation.ValidationException} when it does not apply
     */
    public synchronized void publishChanges(ChangeBatch batch) throws IOException {
        if (closed) throw new IllegalStateException("server closed");
        byte[] encoded = ChangeCodec.encode(batch);
        if (encoded.length + UPDATE_FRAME_OVERHEAD > config.frameMaxBytes) throw new IOException("batch of " + encoded.length + " bytes exceeds the frame limit " + config.frameMaxBytes);
        RefDataStore nextStore = nextStore(batch);
        Snapshot next = encoder.encode(nextStore, snapshot.info().getSource(), System.currentTimeMillis());
        tcp.publishUpdate(next, Envelope.newBuilder().setUpdateBatch(ByteString.copyFrom(encoded)).build());
        snapshot = next;
        published = new InspectionServer.View(nextStore, next.snapshotIdHex());
        health.published(next, report);
        log.info("published data version {} ({} changes), snapshot {}", batch.version(), batch.changes().size(), next.snapshotIdHex());
    }

    /**
     * Explicit full reload, for direct SQL fixes that bypassed the admin API: re-reads the whole database, validates it
     * and publishes it as a new snapshot generation. Readers, inspection and running transfers keep the previous data
     * until the cutover; live consumers are then told to resynchronise and load the new snapshot. With the admin
     * endpoint enabled, admin commits and the outbox relay are held back for the duration, so no committed batch is
     * lost or applied twice. The load uses the startup settings (retries, sanity minimums, validation policy, heap
     * check); the previous data stays in memory until the cutover, so the heap must hold both copies.
     * @throws ReloadRejectedException another reload is running, or the database is behind the served data version
     * @throws ValidationException the database content fails validation (nothing is published)
     * @throws UnsupportedOperationException the server was started from a built store (no database)
     * @throws IOException the database could not be read (nothing is published)
     */
    public ReloadResult reload() throws IOException {
        AdminServer a = admin;
        return a != null ? a.reload() : reloadFromSource();
    }

    private ReloadResult reloadFromSource() throws IOException {
        if (source == null) throw new UnsupportedOperationException("started from a built store: there is no database to reload from");
        if (!reloadLock.tryLock()) throw new ReloadRejectedException("a reload is already in progress");
        long t0 = System.nanoTime();
        String outcome = "failed";
        try {
            synchronized (this) { if (closed) throw new IllegalStateException("server closed"); }
            long before = published.store().dataVersion();
            log.info("full reload started (serving data version {}, snapshot {})", before, snapshot.snapshotIdHex());
            StoreLoader.Loaded loaded;
            try {
                loaded = new StoreLoader(config, phase -> log.info("full reload: {}", phase)).load(source);
            } catch (StoreLoader.StartupException e) {
                if (e.getCause() instanceof ValidationException invalid) { outcome = "rejected"; throw invalid; }
                throw new IOException("reload failed: " + e.getMessage(), e);
            }
            long loadMs = (System.nanoTime() - t0) / 1_000_000;
            Snapshot next = encoder.encode(loaded.store(), loaded.preflight().sourceDescription(), System.currentTimeMillis());
            synchronized (this) {
                if (closed) throw new IllegalStateException("server closed");
                long served = published.store().dataVersion();
                if (loaded.store().dataVersion() < served) {
                    outcome = "rejected";
                    throw new ReloadRejectedException("the database is at data version " + loaded.store().dataVersion() + ", behind the served version " + served
                            + " (were release rows removed?); nothing was published");
                }
                tcp.publishReload(next);
                snapshot = next;
                published = new InspectionServer.View(loaded.store(), next.snapshotIdHex());
                health.published(next, report);
            }
            outcome = "published";
            long totalMs = (System.nanoTime() - t0) / 1_000_000;
            log.info("full reload published data version {} (was {}), snapshot {}, {} rows, load {} ms, total {} ms; live consumers resynchronise",
                    next.dataVersion(), before, next.snapshotIdHex(), loaded.loadedRows().values().stream().mapToLong(Long::longValue).sum(), loadMs, totalMs);
            return new ReloadResult(before, next.dataVersion(), next.snapshotIdHex(), loaded.loadedRows(), loadMs, totalMs);
        } catch (ReloadRejectedException e) {
            outcome = "rejected";
            throw e;
        } finally {
            metrics.reload(outcome, (System.nanoTime() - t0) / 1_000_000);
            reloadLock.unlock();
        }
    }

    /** The store a batch would produce, built with the configured index settings and the FAIL policy. */
    private RefDataStore nextStore(ChangeBatch batch) {
        return batch.apply(published.store(), new RefDataStoreBuilder().denseThreshold(config.indexDenseThreshold));
    }

    private ChangePublisher changePublisher() {
        return new ChangePublisher() {
            @Override public RefDataStore currentStore() { return RefDataServer.this.currentStore(); }
            @Override public void publish(ChangeBatch batch) throws IOException { publishChanges(batch); }
            @Override public RefDataStore preview(ChangeBatch batch) { return nextStore(batch); }
            @Override public ReloadResult reload() throws IOException { return reloadFromSource(); }
        };
    }

    /** Bound admin port, or -1 when live updates are disabled. */
    public int adminPort() { return admin == null ? -1 : admin.port(); }
    /** The store currently served (changes with every published batch). */
    public RefDataStore currentStore() { return published.store(); }

    /** The bound TCP port (the configured one, or the ephemeral one when configured as 0). */
    public synchronized int tcpPort() { return tcp.port(); }
    /** The bound HTTP port. */
    public synchronized int httpPort() { return health.port(); }
    /** Bound inspection port, or -1 when disabled. */
    public int inspectionPort() { return inspection == null ? -1 : inspection.port(); }
    public Snapshot snapshot() { return snapshot; }
    public StartupReport startupReport() { return report; }
    public ServerMetrics metrics() { return metrics; }
    /** The live-update history currently retained for resuming consumers, oldest first. */
    public java.util.List<com.example.refdata.server.net.SessionHandler.LiveUpdate> liveHistory() { return tcp.liveHistory(); }
    public ServerConfig config() { return config; }

    /** Planned shutdown: readiness DOWN, GoAway to every session, drain, close both ports. Idempotent. */
    @Override public void close() {
        synchronized (this) { if (closed) return; closed = true; }
        health.phase("shutting down");
        health.ready(false);
        if (admin != null) admin.close();
        if (inspection != null) inspection.close();
        if (tcp != null) tcp.shutdown();
        health.close();
        terminated.countDown();
        log.info("server stopped");
    }

    /** Blocks until {@link #close()} has completed (from any thread). */
    public void awaitTermination() throws InterruptedException {
        terminated.await();
    }
}
