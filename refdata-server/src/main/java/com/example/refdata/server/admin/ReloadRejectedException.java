package com.example.refdata.server.admin;

/**
 * A full reload was refused without changing what is served: another reload is running, or the database is behind the
 * served data version (publishing it would lose committed batches, or make the relay apply them a second time).
 */
public final class ReloadRejectedException extends RuntimeException {
    public ReloadRejectedException(String message) { super(message); }
}
