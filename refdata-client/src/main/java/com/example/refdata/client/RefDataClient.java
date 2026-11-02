package com.example.refdata.client;

import com.example.refdata.model.inspection.InspectionServer;

import com.example.refdata.contract.Framing;
import com.example.refdata.model.EntityKind;
import com.example.refdata.model.validation.ValidationPolicy;
import com.example.refdata.protocol.v1.Codec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Consumer library entry point (design sections 8.3 and 14.1): connects to one of the configured servers, loads and
 * verifies a snapshot, and returns the indexed in-memory {@link com.example.refdata.model.RefDataStore}.
 *
 * <pre>{@code
 * RefDataClient client = RefDataClient.builder().endpoints("rds-a:7400", "rds-b:7400").clientName("order-router").build();
 * LoadedSnapshot data = client.load();
 * Client c = data.store().client(123456L);
 * }</pre>
 *
 * Loading runs on a dedicated thread; {@link #load()} blocks, {@link #loadAsync()} does not. A client is reusable for
 * repeated loads; {@link #close()} stops keep-alive and optional cache inspection.
 */
public final class RefDataClient implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(RefDataClient.class);
    private static final AtomicInteger LOADER_THREADS = new AtomicInteger();

    private final ClientSettings settings;
    private volatile KeepAliveSession keepAliveSession;
    private volatile InspectionServer.View inspectionView;
    private final InspectionServer inspection;

    private RefDataClient(ClientSettings settings, InspectionServer.Options options) {
        this.settings = settings;
        try { inspection = options == null ? null : new InspectionServer(options, () -> inspectionView); }
        catch (java.io.IOException e) { throw new IllegalStateException("cannot start cache inspection", e); }
    }

    /** Bound inspection port, or -1 when disabled. */
    public int inspectionPort() { return inspection == null ? -1 : inspection.port(); }

    public static Builder builder() { return new Builder(); }

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

    /** {@link #load(LoadedSnapshot)} on a dedicated thread; the future fails with the same exceptions. */
    public CompletableFuture<LoadedSnapshot> loadAsync(LoadedSnapshot knownSnapshot) {
        CompletableFuture<LoadedSnapshot> future = new CompletableFuture<>();
        Thread thread = new Thread(() -> {
            try {
                SnapshotLoader loader = new SnapshotLoader(settings, knownSnapshot);
                LoadedSnapshot result = loader.load();
                replaceKeepAlive(loader.keepAliveSession());
                if (inspection != null) inspectionView = new InspectionServer.View(result.store(),
                        com.example.refdata.contract.Digests.hex(result.info().getSnapshotId().toByteArray()));
                future.complete(result);
            } catch (Throwable t) {
                log.error("snapshot load failed: {}", t.getMessage());
                future.completeExceptionally(t);
            }
        }, "refdata-client-loader-" + LOADER_THREADS.incrementAndGet());
        thread.setDaemon(true);
        thread.start();
        return future;
    }

    private synchronized void replaceKeepAlive(KeepAliveSession next) {
        if (keepAliveSession != null) keepAliveSession.close();
        keepAliveSession = next;
    }

    /** Closes inspection and the kept-alive connection, if any. Loaded stores are unaffected. */
    @Override public void close() { if (inspection != null) inspection.close(); replaceKeepAlive(null); }

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
        private InspectionServer.Options inspectionOptions;
        /** Enable optional read-only cache inspection; default disabled. */
        public Builder inspection(InspectionServer.Options options) {
            inspectionOptions = Objects.requireNonNull(options); return this;
        }
        private Long expectedDataVersion;
        private Long minDataVersion;
        private ValidationPolicy validationPolicy = ValidationPolicy.FAIL;

        private Builder() {}

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
                    expectedDataVersion, minDataVersion, validationPolicy), inspectionOptions);
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
