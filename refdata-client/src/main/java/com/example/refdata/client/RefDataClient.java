package com.example.refdata.client;

import com.example.refdata.contract.Digests;
import com.example.refdata.contract.Framing;
import com.example.refdata.model.ChangeBatch;
import com.example.refdata.model.EntityKind;
import com.example.refdata.model.RefDataStore;
import com.example.refdata.model.inspection.InspectionServer;
import com.example.refdata.model.validation.ValidationPolicy;
import com.example.refdata.protocol.v1.Codec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Consumer library entry point (design sections 8.3 and 14.1): connects to one of the configured servers, loads and
 * verifies a snapshot, and returns the indexed in-memory {@link RefDataStore}.
 *
 * <pre>{@code
 * RefDataClient client = RefDataClient.builder().endpoints("rds-a:7400", "rds-b:7400").clientName("order-router").build();
 * LoadedSnapshot data = client.load();
 * Client c = data.store().client(123456L);
 * }</pre>
 *
 * Loading runs on a dedicated thread; {@link #load()} blocks, {@link #loadAsync()} does not. A client is reusable for
 * repeated loads; {@link #close()} stops keep-alive and optional cache inspection.
 * <p><b>Live updates</b> ({@link Builder#liveUpdates(boolean)}): the first {@link #load()} returns the initial snapshot
 * and starts a subscription that applies every published transaction and, when the connection is lost, reloads a
 * full snapshot; read the latest data with {@link #currentStore()}. A live client may be loaded only once.
 */
public final class RefDataClient implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(RefDataClient.class);
    private static final AtomicInteger LOADER_THREADS = new AtomicInteger();

    private final ClientSettings settings;
    private final boolean liveUpdates;
    private final InspectionServer inspection;
    private final AtomicBoolean subscribed = new AtomicBoolean();
    private volatile boolean closed;
    private volatile SnapshotLoader activeLoader;
    private volatile Thread subscriptionThread;
    private volatile KeepAliveSession keepAliveSession;
    /** The latest published data; replaced as a whole, never mutated. */
    private volatile LoadedSnapshot current;
    private volatile InspectionServer.View inspectionView;

    private RefDataClient(ClientSettings settings, InspectionServer.Options inspectionOptions, boolean liveUpdates) {
        this.settings = settings;
        this.liveUpdates = liveUpdates;
        try { inspection = inspectionOptions == null ? null : new InspectionServer(inspectionOptions, () -> inspectionView); }
        catch (IOException e) { throw new IllegalStateException("cannot start cache inspection", e); }
    }

    public static Builder builder() { return new Builder(); }

    /** Bound inspection port, or -1 when disabled. */
    public int inspectionPort() { return inspection == null ? -1 : inspection.port(); }

    /**
     * The most recently loaded store; with live updates, the latest version including every applied update.
     * @throws IllegalStateException before the first successful load
     */
    public RefDataStore currentStore() {
        LoadedSnapshot s = current;
        if (s == null) throw new IllegalStateException("no snapshot loaded");
        return s.store();
    }

    /**
     * Loads a snapshot: handshake, stream, verify, index, publish.
     * @throws RefDataUnavailableException no server delivered a snapshot within the startup timeout (C-1, C-10)
     * @throws SnapshotIntegrityException  the data failed its checks on every allowed attempt (C-3, C-4)
     * @throws UnsupportedVersionException the servers do not speak our protocol or schema version
     * @throws DataVersionException        no server serves an acceptable data version (C-14)
     * @throws RefDataClientException      the snapshot failed the local validation rules
     */
    public LoadedSnapshot load() { return load(null); }

    /**
     * Like {@link #load()}, but tells the server which snapshot the caller already holds; when the server still serves
     * that snapshot it sends nothing and {@code knownSnapshot}'s store is returned (design section 9.8).
     * @param knownSnapshot the snapshot the caller holds, or null
     */
    public LoadedSnapshot load(LoadedSnapshot knownSnapshot) {
        try {
            return loadAsync(knownSnapshot).join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof RuntimeException cause) throw cause;
            throw e;
        }
    }

    /** {@link #load()} on a dedicated thread; the future fails with the same exceptions. */
    public CompletableFuture<LoadedSnapshot> loadAsync() { return loadAsync(null); }

    /**
     * {@link #load(LoadedSnapshot)} on a dedicated thread; the future fails with the same exceptions. With live
     * updates the thread stays as the subscription after the future completes.
     */
    @SuppressWarnings("try") // The resource restores MDC on every exit path.
    public CompletableFuture<LoadedSnapshot> loadAsync(LoadedSnapshot knownSnapshot) {
        if (closed) throw new IllegalStateException("client closed");
        if (liveUpdates && (knownSnapshot != null || !subscribed.compareAndSet(false, true))) {
            throw new IllegalStateException("live subscription must start once with a full snapshot");
        }
        CompletableFuture<LoadedSnapshot> future = new CompletableFuture<>();
        Map<String, String> callerContext = MDC.getCopyOfContextMap();
        Thread thread = new Thread(() -> {
            try (LoggingContext context = new LoggingContext(callerContext)) {
                initLoggingContext();
                try {
                    if (liveUpdates) runLiveSubscription(future);
                    else future.complete(loadOnce(knownSnapshot));
                } catch (Throwable t) {
                    // Expected failures carry diagnostics to callers; unexpected failures keep their stack trace.
                    if (closed) { future.completeExceptionally(new RefDataClientException("client closed")); return; }
                    if (t instanceof RefDataClientException) log.warn("snapshot load failed: {}", t.getMessage());
                    else log.error("unexpected snapshot load failure", t);
                    future.completeExceptionally(t);
                }
            }
        }, "refdata-client-loader-" + LOADER_THREADS.incrementAndGet());
        if (liveUpdates) subscriptionThread = thread;
        thread.setDaemon(true);
        thread.start();
        return future;
    }

    private void initLoggingContext() {
        MDC.put("rds.client", settings.clientName());
        MDC.put("rds.instance", settings.clientInstance());
        MDC.put("rds.load", Thread.currentThread().getName());
        MDC.remove("rds.endpoint");
        MDC.remove("rds.snapshot");
        MDC.remove("rds.request");
        MDC.remove("rds.session");
    }

    /** A plain load: one snapshot, published as {@link #current} and returned. */
    private LoadedSnapshot loadOnce(LoadedSnapshot knownSnapshot) {
        SnapshotLoader loader = startLoader(knownSnapshot);
        LoadedSnapshot result = loader.load();
        replaceKeepAlive(loader.keepAliveSession());
        publish(result);
        return result;
    }

    /**
     * The live subscription: load a full snapshot (completing {@code firstLoad}), apply updates on the kept-alive
     * connection until it ends, then reload. Failures before the first snapshot fail {@code firstLoad}; later ones
     * are logged and retried after the maximum back-off. Returns when the client is closed.
     */
    private void runLiveSubscription(CompletableFuture<LoadedSnapshot> firstLoad) throws InterruptedException {
        while (!closed) {
            try {
                SnapshotLoader loader = startLoader(null).onLiveUpdates(this::publish, this::applyUpdate);
                LoadedSnapshot result = loader.load();          // publishes through the listener before updates can arrive
                replaceKeepAlive(loader.keepAliveSession());
                firstLoad.complete(result);
                awaitKeepAliveEnd();
                if (!closed) {
                    log.info("live connection ended; reloading the snapshot");
                    Thread.sleep(settings.backoffInitial());
                }
            } catch (RefDataClientException e) {
                if (!firstLoad.isDone() || closed) throw e;
                log.warn("live subscription reload failed, retrying in {}: {}", settings.backoffMax(), e.getMessage());
                Thread.sleep(settings.backoffMax());
            }
        }
    }

    private SnapshotLoader startLoader(LoadedSnapshot knownSnapshot) {
        SnapshotLoader loader = new SnapshotLoader(settings, knownSnapshot);
        activeLoader = loader;
        if (closed) { loader.cancel(); throw new RefDataClientException("client closed"); }
        return loader;
    }

    private void awaitKeepAliveEnd() throws InterruptedException {
        while (!closed && keepAliveSession != null && !keepAliveSession.closed()) Thread.sleep(100);
    }

    /** Makes a loaded snapshot the current data (and the inspection view). */
    private void publish(LoadedSnapshot snapshot) {
        current = snapshot;
        if (inspection != null) inspectionView = new InspectionServer.View(snapshot.store(), Digests.hex(snapshot.info().getSnapshotId().toByteArray()));
    }

    /**
     * Applies one live update on the keep-alive reader thread. The store's data version advances; {@code info()} and
     * {@code stats()} still describe the snapshot the subscription started from. A failure (version gap, invalid
     * result) propagates and closes the connection, which makes the subscription reload.
     */
    private void applyUpdate(ChangeBatch batch) {
        LoadedSnapshot old = current;
        publish(new LoadedSnapshot(batch.apply(old.store()), old.info(), old.stats()));
    }

    private synchronized void replaceKeepAlive(KeepAliveSession next) {
        if (keepAliveSession != null) keepAliveSession.close();
        if (closed && next != null) { next.close(); return; }
        keepAliveSession = next;
    }

    /** Closes inspection, the kept-alive connection and the live subscription, if any. Loaded stores are unaffected. */
    @Override public void close() {
        closed = true;
        SnapshotLoader loader = activeLoader;
        if (loader != null) loader.cancel();
        Thread subscription = subscriptionThread;
        if (subscription != null) subscription.interrupt();
        if (inspection != null) inspection.close();
        replaceKeepAlive(null);
    }

    /** Collects the settings of a {@link RefDataClient}; only {@link #endpoints} and {@link #clientName} are mandatory. */
    public static final class Builder {
        private final List<Endpoint> endpoints = new ArrayList<>();
        private String clientName;
        private String clientInstance;
        private EnumSet<EntityKind> entityTypes = EnumSet.allOf(EntityKind.class);
        private final List<Codec> acceptedCodecs = new ArrayList<>();
        private int maxFrameBytes = Framing.DEFAULT_MAX_FRAME_BYTES;
        private Duration startupTimeout = Duration.ofMinutes(3);
        private Duration connectTimeout = Duration.ofSeconds(5);
        private Duration readTimeout = Duration.ofSeconds(30);
        private Duration backoffInitial = Duration.ofMillis(200);
        private Duration backoffMax = Duration.ofSeconds(10);
        private boolean keepAlive;
        private boolean liveUpdates;
        private InspectionServer.Options inspectionOptions;
        private Long expectedDataVersion;
        private Long minDataVersion;
        private ValidationPolicy validationPolicy = ValidationPolicy.FAIL;

        private Builder() {}

        /**
         * Subscribe to live updates after the first load (implies {@link #keepAlive(boolean) keepAlive}); requires
         * all entity types. Default false.
         */
        public Builder liveUpdates(boolean enabled) { liveUpdates = enabled; if (enabled) keepAlive = true; return this; }

        /** Enable optional read-only cache inspection; default disabled. */
        public Builder inspection(InspectionServer.Options options) {
            inspectionOptions = Objects.requireNonNull(options);
            return this;
        }

        /** Servers as {@code host:port}, tried in this order. */
        public Builder endpoints(String... hostPorts) {
            for (String hp : hostPorts) endpoints.add(Endpoint.parse(hp));
            return this;
        }

        public Builder endpoints(Collection<Endpoint> list) { endpoints.addAll(list); return this; }
        /** Application name, for the server's logs and metrics. */
        public Builder clientName(String name) { this.clientName = name; return this; }
        /** Instance name; default {@code host/pid}. */
        public Builder clientInstance(String instance) { this.clientInstance = instance; return this; }

        /** The entity types to load; default all. COUNTRY and CURRENCY are always added to a subset. */
        public Builder entityTypes(EntityKind... kinds) { return entityTypes(Arrays.asList(kinds)); }

        public Builder entityTypes(Collection<EntityKind> kinds) {
            if (kinds.isEmpty()) throw new IllegalArgumentException("at least one entity type is required");
            this.entityTypes = EnumSet.copyOf(kinds);
            return this;
        }

        /** Payload codecs the client accepts, in order of preference; default none (uncompressed). */
        public Builder acceptedCodecs(Codec... codecs) {
            acceptedCodecs.clear();
            for (Codec c : codecs) {
                if (c == Codec.CODEC_UNSPECIFIED || c == Codec.UNRECOGNIZED) throw new IllegalArgumentException("not a codec: " + c);
                acceptedCodecs.add(c);
            }
            return this;
        }

        /** Largest frame accepted; default {@link Framing#DEFAULT_MAX_FRAME_BYTES}, which every server stays within. */
        public Builder maxFrameBytes(int bytes) {
            if (bytes < Framing.DEFAULT_MAX_FRAME_BYTES) throw new IllegalArgumentException("clients must accept at least " + Framing.DEFAULT_MAX_FRAME_BYTES + " bytes");
            this.maxFrameBytes = bytes;
            return this;
        }

        /** Total time {@link RefDataClient#load()} may take, retries included; default 3 minutes. */
        public Builder startupTimeout(Duration d) { this.startupTimeout = positive(d, "startupTimeout"); return this; }
        public Builder connectTimeout(Duration d) { this.connectTimeout = positive(d, "connectTimeout"); return this; }
        /** Longest silence tolerated on a connection; raised at run time if it does not cover 3 heartbeat intervals. Default 30 s. */
        public Builder readTimeout(Duration d) { this.readTimeout = positive(d, "readTimeout"); return this; }

        /** Exponential back-off with jitter between connection attempts; default 200 ms doubling up to 10 s. */
        public Builder backoff(Duration initial, Duration max) {
            this.backoffInitial = positive(initial, "backoff initial");
            this.backoffMax = positive(max, "backoff max");
            if (max.compareTo(initial) < 0) throw new IllegalArgumentException("backoff max is below initial");
            return this;
        }

        /** Keep the connection after the load and exchange heartbeats on it; default false. */
        public Builder keepAlive(boolean keepAlive) { this.keepAlive = keepAlive; return this; }
        /** Only accept servers whose data version is exactly this (C-14). */
        public Builder expectedDataVersion(long version) { this.expectedDataVersion = version; return this; }
        /** Only accept servers whose data version is at least this (C-14). */
        public Builder minDataVersion(long version) { this.minDataVersion = version; return this; }
        /** What to do when the received data breaks a model rule; default FAIL. */
        public Builder validationPolicy(ValidationPolicy policy) { this.validationPolicy = Objects.requireNonNull(policy); return this; }

        public RefDataClient build() {
            if (liveUpdates && (!keepAlive || entityTypes.size() != EntityKind.values().length)) throw new IllegalStateException("live updates require keepAlive and all entity types");
            if (endpoints.isEmpty()) throw new IllegalStateException("at least one endpoint is required");
            if (clientName == null || clientName.isBlank()) throw new IllegalStateException("clientName is required");
            EnumSet<EntityKind> kinds = EnumSet.copyOf(entityTypes);
            List<Integer> wireTypes = new ArrayList<>();
            if (kinds.size() < EntityKind.values().length) {
                kinds.add(EntityKind.COUNTRY);
                kinds.add(EntityKind.CURRENCY);
                for (EntityKind k : kinds) wireTypes.add(k.wireCode());
            }
            return new RefDataClient(new ClientSettings(List.copyOf(endpoints), clientName,
                    clientInstance == null ? defaultInstance() : clientInstance, List.copyOf(wireTypes), List.copyOf(acceptedCodecs),
                    maxFrameBytes, startupTimeout, connectTimeout, readTimeout, backoffInitial, backoffMax, keepAlive,
                    expectedDataVersion, minDataVersion, validationPolicy), inspectionOptions, liveUpdates);
        }

        private static Duration positive(Duration d, String name) {
            if (d == null || !d.isPositive()) throw new IllegalArgumentException(name + " must be positive");
            return d;
        }

        private static String defaultInstance() {
            String host;
            try {
                host = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                host = "localhost";
            }
            return host + "/" + ProcessHandle.current().pid();
        }
    }
}
