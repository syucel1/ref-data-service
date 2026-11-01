package com.example.refdata.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * An embedded Apache Derby database, on disk or in memory. Opens (creating if needed) on construction; {@link #close()}
 * shuts the database down cleanly (and drops it when it is in-memory). The Derby driver auto-registers with
 * {@link DriverManager} when the derby jar is on the class path.
 */
public final class EmbeddedDerby implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(EmbeddedDerby.class);
    /** SQLState Derby reports for a successful single-database shutdown. */
    private static final String SHUTDOWN_OK = "08006";

    private final String databaseName;
    private final boolean inMemory;

    private EmbeddedDerby(String databaseName, boolean inMemory) throws SQLException {
        this.databaseName = databaseName;
        this.inMemory = inMemory;
        DriverManager.getConnection(url() + ";create=true").close();
        log.debug("opened Derby database {}", url());
    }

    /** Opens or creates a database stored in the given directory. */
    public static EmbeddedDerby onDisk(Path directory) throws SQLException {
        return new EmbeddedDerby(directory.toAbsolutePath().toString(), false);
    }

    /** Creates an in-memory database with the given name (dropped on {@link #close()}). */
    public static EmbeddedDerby inMemory(String name) throws SQLException {
        return new EmbeddedDerby("memory:" + name, true);
    }

    /** The JDBC URL of this database, without any attribute. */
    public String url() {
        return "jdbc:derby:" + databaseName;
    }

    /** A new connection in auto-commit mode. */
    public Connection connect() throws SQLException {
        return DriverManager.getConnection(url());
    }

    /** Shuts the database down; an in-memory database is dropped. Safe to call twice. */
    @Override
    public void close() {
        String attribute = inMemory ? ";drop=true" : ";shutdown=true";
        try {
            DriverManager.getConnection(url() + attribute).close();
            log.warn("Derby did not confirm shutdown of {}", url());
        } catch (SQLException e) {
            if (SHUTDOWN_OK.equals(e.getSQLState())) log.debug("closed Derby database {}", url());
            else if ("XJ004".equals(e.getSQLState())) log.debug("Derby database {} was already closed", url());
            else log.warn("shutdown of {} failed: {} ({})", url(), e.getMessage(), e.getSQLState());
        }
    }
}
