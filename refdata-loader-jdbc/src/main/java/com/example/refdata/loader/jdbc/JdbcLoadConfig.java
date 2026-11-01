package com.example.refdata.loader.jdbc;

/**
 * Tuning of a {@link JdbcRefDataSource}.
 *
 * @param fetchSize   JDBC fetch size for the streaming table reads (design: 10,000)
 * @param parallelism how many tables are read at once, each on its own connection and thread (design: 4)
 * @param readOnly    whether the connections are marked read-only (the service's database user should be read-only anyway)
 */
public record JdbcLoadConfig(int fetchSize, int parallelism, boolean readOnly) {
    public static final int DEFAULT_FETCH_SIZE = 10_000;
    public static final int DEFAULT_PARALLELISM = 4;

    public JdbcLoadConfig {
        if (fetchSize < 1) throw new IllegalArgumentException("fetchSize must be >= 1: " + fetchSize);
        if (parallelism < 1) throw new IllegalArgumentException("parallelism must be >= 1: " + parallelism);
    }

    /** Fetch size 10,000, four tables in parallel, read-only connections. */
    public static JdbcLoadConfig defaults() {
        return new JdbcLoadConfig(DEFAULT_FETCH_SIZE, DEFAULT_PARALLELISM, true);
    }
}
