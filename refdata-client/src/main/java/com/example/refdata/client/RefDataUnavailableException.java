package com.example.refdata.client;

/**
 * No snapshot could be loaded within the startup timeout: every host unreachable, or the load was still in the named
 * {@link #phase()} when the timeout expired (design rows C-1 and C-10).
 */
public final class RefDataUnavailableException extends RefDataClientException {
    private final LoadPhase phase;

    public RefDataUnavailableException(LoadPhase phase, String message) { this(phase, message, null); }

    public RefDataUnavailableException(LoadPhase phase, String message, Throwable cause) {
        super("reference data unavailable (phase " + phase + "): " + message, cause);
        this.phase = phase;
    }

    /** The phase the load was in when it gave up. */
    public LoadPhase phase() { return phase; }
}
