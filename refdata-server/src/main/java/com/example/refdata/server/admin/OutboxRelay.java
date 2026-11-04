package com.example.refdata.server.admin;

import com.example.refdata.contract.ChangeCodec;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Publishes committed batches from {@code REFDATA_OUTBOX} in version order: every row with a version above the served
 * data version is decoded and handed to the {@link ChangePublisher}. Runs after every admin commit and on a fixed
 * schedule, so batches committed by an earlier process (or lost to a crash between commit and publication) are still
 * published. Rows well behind the served version are deleted.
 * <p>{@link #drain()} is synchronized; the admin writer holds the same lock around validate-commit-publish.
 */
final class OutboxRelay {
    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    /** Batches published per drain, so a long backlog cannot hold the lock for long. */
    static final int MAX_BATCHES_PER_DRAIN = 32;
    /** Outbox rows kept behind the served version (diagnostics only; consumers never read them). */
    static final int RETAINED_ROWS = 32;

    private final ConnectionFactory connections;
    private final ChangePublisher publisher;
    private String lastFailure;

    OutboxRelay(ConnectionFactory connections, ChangePublisher publisher) {
        this.connections = connections;
        this.publisher = publisher;
    }

    /** Publishes the pending batches; throws on the first failure (the rest stay in the outbox for the next drain). */
    synchronized void drain() throws SQLException, IOException {
        try (Connection c = connections.open()) {
            long served = publisher.currentStore().dataVersion();
            try (PreparedStatement s = c.prepareStatement("SELECT payload FROM " + ChangeWriter.OUTBOX_TABLE + " WHERE version > ? ORDER BY version")) {
                s.setLong(1, served);
                s.setMaxRows(MAX_BATCHES_PER_DRAIN);
                try (ResultSet rs = s.executeQuery()) {
                    while (rs.next()) publisher.publish(ChangeCodec.decode(rs.getBytes(1)));
                }
            }
            try (PreparedStatement cleanup = c.prepareStatement("DELETE FROM " + ChangeWriter.OUTBOX_TABLE + " WHERE version < ?")) {
                cleanup.setLong(1, publisher.currentStore().dataVersion() - RETAINED_ROWS);
                cleanup.executeUpdate();
            }
        }
    }

    /** The scheduled variant: never throws, logs a failure once (until it changes or clears). */
    void drainQuietly() {
        try {
            drain();
            if (lastFailure != null) log.info("outbox relay recovered");
            lastFailure = null;
        } catch (Exception e) {
            String failure = e.toString();
            if (!Objects.equals(failure, lastFailure)) log.error("outbox relay cannot publish pending batches (will retry)", e);
            lastFailure = failure;
        }
    }
}
