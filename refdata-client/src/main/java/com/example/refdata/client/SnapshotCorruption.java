package com.example.refdata.client;

/** The staged snapshot cannot be trusted (hash, totals, malformed batch): discard it and start over (design row C-4). */
final class SnapshotCorruption extends Exception {
    SnapshotCorruption(String message) { super(message); }
    SnapshotCorruption(String message, Throwable cause) { super(message, cause); }
}
