package com.example.refdata.client;

/** The snapshot failed its hash or total checks, or a chunk failed repeatedly, on every allowed restart (design row C-4). */
public final class SnapshotIntegrityException extends RefDataClientException {
    public SnapshotIntegrityException(String message) { super(message); }
    public SnapshotIntegrityException(String message, Throwable cause) { super(message, cause); }
}
