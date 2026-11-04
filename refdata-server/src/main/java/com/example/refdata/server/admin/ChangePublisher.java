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
}
