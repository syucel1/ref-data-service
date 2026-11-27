package com.example.refdata.server.admin;

import com.example.refdata.model.ChangeBatch;
import com.example.refdata.model.RefDataStore;
import java.io.IOException;

/** What the admin side needs from the running service: the published store and a way to publish the next batch. */
public interface ChangePublisher {
    /** The store currently served to consumers. */
    RefDataStore currentStore();

    /**
     * Validates and publishes a batch that is already committed to the database: next store, next snapshot, and an
     * {@code update_batch} to every live session.
     */
    void publish(ChangeBatch batch) throws IOException;

    /** Builds the store that {@link #publish} would produce, without publishing it (validation only). */
    RefDataStore preview(ChangeBatch batch);

    /**
     * Loads, validates and publishes the whole database as a new snapshot generation (live consumers resynchronise).
     * The caller guarantees no batch is published concurrently.
     * @throws ReloadRejectedException another reload is running, or the database is behind the served version
     * @throws com.example.refdata.model.validation.ValidationException the database content fails validation
     * @throws UnsupportedOperationException the service was not started from a database
     * @throws IOException the database could not be read
     */
    default ReloadResult reload() throws IOException {
        throw new UnsupportedOperationException("full reload is not supported by this publisher");
    }
}
