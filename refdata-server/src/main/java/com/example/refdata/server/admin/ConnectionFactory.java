package com.example.refdata.server.admin;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/** Opens a new connection to the reference database for the admin writer and the outbox relay. */
@FunctionalInterface
public interface ConnectionFactory {
    Connection open() throws SQLException;

    /** Connects through {@link DriverManager}; an empty user means "credentials in the URL, or none". */
    static ConnectionFactory driverManager(String url, String user, String password) {
        return user == null || user.isEmpty() ? () -> DriverManager.getConnection(url) : () -> DriverManager.getConnection(url, user, password);
    }
}
