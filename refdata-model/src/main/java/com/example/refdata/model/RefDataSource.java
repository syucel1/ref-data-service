package com.example.refdata.model;

/**
 * Where the reference data comes from. The only thing the rest of the system knows about the database:
 * an implementation streams every row of the six tables into a sink, in primary-key order, from one consistent view.
 * The JDBC implementation lives in refdata-loader-jdbc; a Hibernate one can implement the same interface.
 */
public interface RefDataSource {

    /** Row counts per kind and the data version, read cheaply before the full load (for pre-flight checks). */
    Preflight preflight() throws Exception;

    /** Streams all rows into the sink. Returns the data version the rows were read at. */
    long load(RefDataSink sink) throws Exception;

    record Preflight(java.util.Map<EntityKind, Long> rowCounts, long dataVersion, String sourceDescription) {
    }
}
