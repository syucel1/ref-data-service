package com.example.refdata.loader.jdbc;

import com.example.refdata.model.EntityKind;
import com.example.refdata.model.RefDataSink;
import com.example.refdata.model.RefDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * {@link RefDataSource} over plain JDBC. Each of the six tables is streamed in primary-key order on its own connection
 * and thread (at most {@link JdbcLoadConfig#parallelism()} at a time) inside a read-only REPEATABLE READ transaction,
 * with the configured fetch size; rows are mapped to the model records and handed to the sink as they arrive.
 * Repeated low-cardinality strings (codes, cities, regions, BICs) are de-duplicated per table so the model holds one
 * instance per distinct value. The latest PUBLISHED release id is read before and after the tables; if it changed,
 * the load fails so the caller can retry from a consistent state.
 * <p>
 * Sink contract: each table is pushed by its own thread, so the sink's six methods may be called concurrently with
 * each other, but no single method is ever called from two threads at once, and a method is never called again
 * after {@code load} returns. {@code RefDataStoreBuilder} is written to this contract.
 */
public final class JdbcRefDataSource implements RefDataSource {
    private static final Logger log = LoggerFactory.getLogger(JdbcRefDataSource.class);
    private static final String RELEASE_SQL = "SELECT MAX(release_id) FROM REFDATA_RELEASE WHERE status = 'PUBLISHED'";
    /** Derby's SQLState for "table does not exist" (standard SQL uses 42S02; both are accepted). */
    private static final List<String> NO_SUCH_TABLE = List.of("42X05", "42S02");

    private final Supplier<Connection> connections;
    private final String description;
    private final JdbcLoadConfig config;

    /** Connects through {@link DriverManager} with the given URL (no credentials; pass them in the URL if the driver needs them). */
    public JdbcRefDataSource(String url, JdbcLoadConfig config) {
        this(() -> {
            try {
                return DriverManager.getConnection(url);
            } catch (SQLException e) {
                throw new IllegalStateException("cannot connect to " + stripCredentials(url) + ": " + e.getMessage(), e);
            }
        }, stripCredentials(url), config);
    }

    /**
     * @param connections yields a fresh connection per call (a pool's {@code getConnection}, wrapped); the source closes every connection it takes
     * @param description shown in logs and the pre-flight report, must not contain credentials
     */
    public JdbcRefDataSource(Supplier<Connection> connections, String description, JdbcLoadConfig config) {
        this.connections = connections;
        this.description = description;
        this.config = config;
    }

    /** Removes {@code user=}/{@code password=} attributes and any {@code user:password@} authority from a JDBC URL. */
    static String stripCredentials(String url) {
        return url.replaceAll("(?i)(password|user)=[^;&]*", "$1=***").replaceAll("//[^/@]*@", "//***@");
    }

    @Override
    public Preflight preflight() throws JdbcLoadException {
        try (Connection c = open()) {
            Map<EntityKind, Long> counts = new EnumMap<>(EntityKind.class);
            try (Statement st = c.createStatement()) {
                for (EntityKind kind : EntityKind.values()) {
                    try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + kind.tableName())) {
                        rs.next();
                        counts.put(kind, rs.getLong(1));
                    }
                }
            }
            long release = releaseId(c);
            c.commit();
            log.info("pre-flight of {}: rows {}, release {}", description, counts, release);
            return new Preflight(counts, release, description);
        } catch (SQLException e) {
            throw new JdbcLoadException("pre-flight of " + description + " failed: " + e.getMessage(), e);
        }
    }

    @Override
    public long load(RefDataSink sink) throws JdbcLoadException {
        long start = System.nanoTime();
        long releaseBefore;
        try (Connection c = open()) {
            releaseBefore = releaseId(c);
            c.commit();
        } catch (SQLException e) {
            throw new JdbcLoadException("reading the release id from " + description + " failed: " + e.getMessage(), e);
        }

        int threads = Math.min(config.parallelism(), EntityKind.values().length);
        AtomicInteger threadNo = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(threads, r -> {
            Thread t = new Thread(r, "refdata-load-" + threadNo.incrementAndGet());
            t.setDaemon(true);
            return t;
        });
        Map<EntityKind, Long> loaded = new EnumMap<>(EntityKind.class);
        try {
            List<Future<Long>> futures = new ArrayList<>();
            for (EntityKind kind : EntityKind.values()) futures.add(pool.submit(() -> loadTable(kind, sink)));
            JdbcLoadException failure = null;
            for (int i = 0; i < futures.size(); i++) {
                EntityKind kind = EntityKind.values()[i];
                try {
                    loaded.put(kind, futures.get(i).get());
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    if (failure == null) {
                        failure = cause instanceof JdbcLoadException jle ? jle : new JdbcLoadException("loading " + kind.tableName() + " failed: " + cause, cause);
                        for (Future<Long> f : futures) f.cancel(true);
                    } else if (!(cause instanceof java.util.concurrent.CancellationException)) {
                        failure.addSuppressed(cause);
                    }
                } catch (java.util.concurrent.CancellationException e) {
                    // cancelled after another table failed; that failure is reported
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    for (Future<Long> f : futures) f.cancel(true);
                    throw new JdbcLoadException("interrupted while loading " + description, e);
                }
            }
            if (failure != null) throw failure;
        } finally {
            pool.shutdownNow();
            try {
                if (!pool.awaitTermination(30, TimeUnit.SECONDS)) log.warn("load threads of {} did not stop within 30 s", description);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        long releaseAfter;
        try (Connection c = open()) {
            releaseAfter = releaseId(c);
            c.commit();
        } catch (SQLException e) {
            throw new JdbcLoadException("re-reading the release id from " + description + " failed: " + e.getMessage(), e);
        }
        if (releaseAfter != releaseBefore) {
            throw new JdbcLoadException("data version of " + description + " changed during the load (release " + releaseBefore
                    + " before, " + releaseAfter + " after): the rows read are not a consistent set, retry the load");
        }
        log.info("loaded {} from {} in {} s at release {}", loaded, description, String.format(Locale.ROOT, "%.1f", (System.nanoTime() - start) / 1e9), releaseAfter);
        return releaseAfter;
    }

    /** Streams one table into the sink on its own connection; returns the row count. */
    private long loadTable(EntityKind kind, RefDataSink sink) throws JdbcLoadException {
        RowMappers.TableReader reader = RowMappers.forKind(kind);
        RowMappers.Row row = new RowMappers.Row();
        long start = System.nanoTime();
        long rows = 0;
        try (Connection c = open(); Statement st = c.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            st.setFetchSize(config.fetchSize());
            try (ResultSet rs = st.executeQuery(reader.sql())) {
                while (rs.next()) {
                    reader.push(rs, row, sink);
                    rows++;
                    if (Thread.currentThread().isInterrupted()) throw new JdbcLoadException("loading " + kind.tableName() + " was cancelled after " + rows + " rows");
                }
            }
            c.commit();
            log.info("{}: {} rows in {} s", kind.tableName(), rows, String.format(Locale.ROOT, "%.1f", (System.nanoTime() - start) / 1e9));
            return rows;
        } catch (SQLException e) {
            throw new JdbcLoadException("loading " + kind.tableName() + " from " + description + " failed after " + rows + " rows: " + e.getMessage() + " (SQLState " + e.getSQLState() + ")", e);
        } catch (RuntimeException e) {
            throw new JdbcLoadException("loading " + kind.tableName() + " from " + description + " failed after " + rows + " rows (row " + (rows + 1) + "): " + e, e);
        }
    }

    /** A connection prepared for a consistent read: auto-commit off, read-only if configured, REPEATABLE READ. */
    private Connection open() throws SQLException {
        Connection c;
        try {
            c = connections.get();
        } catch (RuntimeException e) {
            throw new SQLException(e.getMessage(), e);
        }
        try {
            c.setAutoCommit(false);
            c.setReadOnly(config.readOnly());
            c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            return c;
        } catch (SQLException | RuntimeException e) {
            try { c.close(); } catch (SQLException suppressed) { e.addSuppressed(suppressed); }
            throw e;
        }
    }

    /** Latest PUBLISHED release id; 0 when the table is empty or missing. */
    private static long releaseId(Connection c) throws SQLException {
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(RELEASE_SQL)) {
            return rs.next() ? rs.getLong(1) : 0L;     // MAX over no rows is NULL -> getLong gives 0
        } catch (SQLException e) {
            if (NO_SUCH_TABLE.contains(e.getSQLState())) {
                log.warn("no REFDATA_RELEASE table: data version is 0");
                return 0L;
            }
            throw e;
        }
    }
}
