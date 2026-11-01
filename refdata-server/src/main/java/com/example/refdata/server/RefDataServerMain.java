package com.example.refdata.server;

import com.example.refdata.loader.jdbc.JdbcLoadConfig;
import com.example.refdata.loader.jdbc.JdbcRefDataSource;
import com.example.refdata.model.RefDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.sql.DriverManager;

/**
 * Launcher: {@code java ... RefDataServerMain [config.properties]}. Reads the configuration (file + {@code -Drds.*}),
 * wires the JDBC source from {@code rds.db.url} (and {@code rds.db.user}/{@code rds.db.password} when set), runs
 * {@link RefDataServer#start}, and shuts down cleanly on SIGTERM. Exits with status 1 and a clear message on any
 * startup failure.
 */
public final class RefDataServerMain {
    private static final Logger log = LoggerFactory.getLogger(RefDataServerMain.class);

    private RefDataServerMain() {}

    public static void main(String[] args) {
        if (args.length > 1) {
            System.err.println("usage: RefDataServerMain [config.properties]   (-Drds.* overrides)");
            System.exit(2);
        }
        ServerConfig config;
        try {
            config = args.length == 1 ? ServerConfig.load(Path.of(args[0])) : ServerConfig.load();
            if (config.dbUrl.isEmpty()) throw new ServerConfig.ConfigException("rds.db.url is required");
        } catch (ServerConfig.ConfigException e) {
            fail("configuration error: " + e.getMessage(), null);
            return;
        }
        RefDataServer server;
        try {
            server = RefDataServer.start(config, source(config));
        } catch (StoreLoader.StartupException | IllegalStateException e) {
            fail("startup failed: " + e.getMessage(), e);
            return;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail("startup interrupted", null);
            return;
        }
        Thread hook = new Thread(() -> { log.info("shutdown signal received"); server.close(); }, "rds-shutdown");
        Runtime.getRuntime().addShutdownHook(hook);
        try {
            server.awaitTermination();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            server.close();
        }
    }

    private static RefDataSource source(ServerConfig config) {
        JdbcLoadConfig load = new JdbcLoadConfig(config.loadFetchSize, config.loadParallelism, true);
        if (config.dbUser.isEmpty()) return new JdbcRefDataSource(config.dbUrl, load);
        String url = config.dbUrl, user = config.dbUser, password = config.dbPassword();
        return new JdbcRefDataSource(() -> {
            try {
                return DriverManager.getConnection(url, user, password);
            } catch (java.sql.SQLException e) {
                throw new IllegalStateException("cannot connect to " + url + " as " + user + ": " + e.getMessage(), e);
            }
        }, url + " (user " + user + ")", load);
    }

    private static void fail(String message, Throwable cause) {
        log.error(message, cause);
        System.err.println("refdata-server: " + message);
        System.exit(1);
    }
}
