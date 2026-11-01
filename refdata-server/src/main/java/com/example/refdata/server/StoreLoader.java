package com.example.refdata.server;

import com.example.refdata.model.EntityKind;
import com.example.refdata.model.RefDataSource;
import com.example.refdata.model.RefDataStore;
import com.example.refdata.model.RefDataStoreBuilder;
import com.example.refdata.model.validation.ValidationException;
import com.example.refdata.model.validation.ValidationReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Steps 2-6 of the startup sequence (DESIGN.md 7.1): pre-flight with connection retries and back-off (S-2), sanity
 * minimums (S-9), heap estimate (S-3), the full load with retries (S-4), then validation and index building (S-5..S-8,
 * S-14). Any failure is a {@link StartupException} with a message that names the cause.
 */
public final class StoreLoader {
    private static final Logger log = LoggerFactory.getLogger(StoreLoader.class);

    /** Startup cannot continue; the message is meant for the operator. */
    public static final class StartupException extends RuntimeException {
        public StartupException(String message) { super(message); }
        public StartupException(String message, Throwable cause) { super(message, cause); }
    }

    /** What loading produced. */
    public record Loaded(RefDataStore store, ValidationReport validation, RefDataSource.Preflight preflight, Map<EntityKind, Long> loadedRows,
                         long loadMs, long buildMs, int loadAttempts, long heapEstimateBytes, String heapAssumptions) {
    }

    /**
     * Estimated heap bytes per row of the in-memory model, calibrated on a measured medium profile (1.13 M rows held
     * 311 MiB: addresses weigh about twice the other kinds), and per record on the wire (design 15.1, which matched).
     */
    static final Map<EntityKind, Integer> MODEL_BYTES_PER_ROW = new EnumMap<>(Map.of(
            EntityKind.COUNTRY, 320, EntityKind.CURRENCY, 320, EntityKind.CLIENT, 320, EntityKind.ADDRESS, 480,
            EntityKind.ACCOUNT, 220, EntityKind.SETTLEMENT_INSTRUCTION, 240));
    static final Map<EntityKind, Integer> WIRE_BYTES_PER_ROW = new EnumMap<>(Map.of(
            EntityKind.COUNTRY, 40, EntityKind.CURRENCY, 30, EntityKind.CLIENT, 101, EntityKind.ADDRESS, 87,
            EntityKind.ACCOUNT, 60, EntityKind.SETTLEMENT_INSTRUCTION, 74));
    /** Indexes as a fraction of the model (measured ~0.11; kept conservative). */
    private static final double INDEX_FRACTION = 0.2;
    /**
     * Peak over the live estimate: validation and index building were measured at +65 % over the model, plus GC
     * headroom; deliberately on the safe side so that a heap sized to the estimate does not fail late (design S-3).
     */
    static final double PEAK_FACTOR = 1.8;

    private final ServerConfig config;
    private final Duration firstRetryDelay;
    private final Consumer<String> phase;

    /** @param phase receives the current phase name (for the readiness endpoint) */
    public StoreLoader(ServerConfig config, Consumer<String> phase) {
        this(config, Duration.ofSeconds(4), phase);
    }

    StoreLoader(ServerConfig config, Duration firstRetryDelay, Consumer<String> phase) {
        this.config = config;
        this.firstRetryDelay = firstRetryDelay;
        this.phase = phase;
    }

    public Loaded load(RefDataSource source) {
        phase.accept("connecting");
        RefDataSource.Preflight pre = preflightWithRetries(source);
        checkMinimumRows(pre.rowCounts());
        long heapEstimate = checkHeap(pre.rowCounts(), Runtime.getRuntime().maxMemory());

        phase.accept("loading");
        int attempts = config.loadRetries + 1;
        Exception last = null;
        for (int attempt = 1; attempt <= attempts; attempt++) {
            RefDataStoreBuilder builder = new RefDataStoreBuilder().policy(config.validationPolicy).denseThreshold(config.indexDenseThreshold);
            for (Map.Entry<EntityKind, Long> e : config.tableMinRows.entrySet()) builder.minRows(e.getKey(), e.getValue());
            for (Map.Entry<EntityKind, Long> e : pre.rowCounts().entrySet()) builder.expect(e.getKey(), e.getValue());
            long t0 = System.nanoTime();
            long version;
            try {
                version = source.load(builder);
            } catch (Exception e) {
                last = e;
                log.warn("load attempt {}/{} failed: {}", attempt, attempts, e.toString());
                continue;
            }
            long loadMs = (System.nanoTime() - t0) / 1_000_000;
            if (version != pre.dataVersion()) {
                last = new StartupException("data version changed during the load: pre-flight " + pre.dataVersion() + ", after load " + version);
                log.warn("load attempt {}/{}: {}", attempt, attempts, last.getMessage());
                continue;
            }
            builder.dataVersion(version);
            Map<EntityKind, Long> collected = new EnumMap<>(EntityKind.class);
            for (EntityKind k : EntityKind.values()) collected.put(k, (long) builder.collected(k));
            log.info("loaded {} rows in {} ms: {}", collected.values().stream().mapToLong(Long::longValue).sum(), loadMs, collected);

            phase.accept("validating");
            long t1 = System.nanoTime();
            RefDataStoreBuilder.Result result;
            try {
                result = builder.build();
            } catch (ValidationException e) {
                throw new StartupException("the loaded data failed validation (policy " + config.validationPolicy + "):\n" + e.report(), e);
            }
            long buildMs = (System.nanoTime() - t1) / 1_000_000;
            Map<EntityKind, Long> loaded = new EnumMap<>(EntityKind.class);
            for (EntityKind k : EntityKind.values()) loaded.put(k, (long) result.store().count(k));
            if (!result.report().isClean()) log.warn("validation with policy {}: {}", config.validationPolicy, result.report());
            log.info("validated and indexed in {} ms", buildMs);
            return new Loaded(result.store(), result.report(), pre, loaded, loadMs, buildMs, attempt, heapEstimate, heapAssumptions());
        }
        throw new StartupException("loading failed after " + attempts + " attempt(s): " + (last == null ? "unknown" : last.getMessage()), last);
    }

    private RefDataSource.Preflight preflightWithRetries(RefDataSource source) {
        int attempts = config.dbConnectRetries + 1;
        Duration delay = firstRetryDelay;
        Exception last = null;
        for (int attempt = 1; attempt <= attempts; attempt++) {
            try {
                return source.preflight();
            } catch (Exception e) {
                last = e;
                if (attempt < attempts) {
                    log.warn("database pre-flight attempt {}/{} failed ({}); retrying in {} ms", attempt, attempts, e.getMessage(), delay.toMillis());
                    try { Thread.sleep(delay.toMillis()); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); throw new StartupException("interrupted while waiting to retry the database connection", ie); }
                    delay = delay.multipliedBy(2);
                }
            }
        }
        throw new StartupException("database unreachable after " + attempts + " attempt(s): " + last.getMessage(), last);
    }

    private void checkMinimumRows(Map<EntityKind, Long> counts) {
        for (EntityKind k : EntityKind.values()) {
            long min = config.tableMinRows.getOrDefault(k, 0L);
            long n = counts.getOrDefault(k, 0L);
            if (n < min) throw new StartupException("table " + k.tableName() + " has " + n + " rows, below the sanity minimum " + min + " (rds.table.min.rows." + k.tableName().toLowerCase(java.util.Locale.ROOT) + ")");
        }
    }

    /** Estimates the live heap for the model, the indexes and the encoded frames (one copy per codec) and fails when it does not fit. */
    long checkHeap(Map<EntityKind, Long> counts, long maxHeap) {
        long estimate = estimateHeap(counts);
        log.info("heap estimate {} MiB for {} rows ({}), -Xmx {} MiB", estimate >> 20, counts.values().stream().mapToLong(Long::longValue).sum(), heapAssumptions(), maxHeap >> 20);
        if (estimate > maxHeap) {
            throw new StartupException("configured heap is too small: estimated " + (estimate >> 20) + " MiB needed, " + (maxHeap >> 20) + " MiB available (-Xmx); assumed " + heapAssumptions());
        }
        return estimate;
    }

    /** The figures behind {@link #estimateHeap}, for the startup report. */
    String heapAssumptions() {
        StringBuilder sb = new StringBuilder("model bytes/row ");
        for (EntityKind k : EntityKind.values()) sb.append(k.name().toLowerCase(java.util.Locale.ROOT)).append('=').append(MODEL_BYTES_PER_ROW.get(k)).append(' ');
        sb.append("+ indexes ").append((int) (INDEX_FRACTION * 100)).append("% of model + frames (wire bytes/row ");
        for (EntityKind k : EntityKind.values()) sb.append(k.name().toLowerCase(java.util.Locale.ROOT)).append('=').append(WIRE_BYTES_PER_ROW.get(k)).append(' ');
        sb.append("x ").append(1 + config.codecs.size()).append(" codec(s)), all x peak factor ").append(PEAK_FACTOR);
        return sb.toString();
    }

    long estimateHeap(Map<EntityKind, Long> counts) {
        long model = 0, wire = 0;
        for (EntityKind k : EntityKind.values()) {
            long n = counts.getOrDefault(k, 0L);
            model += n * MODEL_BYTES_PER_ROW.get(k);
            wire += n * WIRE_BYTES_PER_ROW.get(k);
        }
        long indexes = (long) (model * INDEX_FRACTION);
        long frames = wire * (1 + config.codecs.size());
        return (long) ((model + indexes + frames) * PEAK_FACTOR);
    }
}
