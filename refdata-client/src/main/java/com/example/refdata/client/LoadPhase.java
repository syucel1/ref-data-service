package com.example.refdata.client;

/** The phases of a load, in order; named in timeouts and reported in {@link LoadStats}. */
public enum LoadPhase {
    /** Opening a TCP connection (includes back-off waits between attempts). */
    CONNECTING,
    /** Hello sent, waiting for HelloAck. */
    HANDSHAKE,
    /** Request sent, waiting for SnapshotBegin (the server may have queued it). */
    QUEUED,
    /** Receiving and checking chunks. */
    STREAMING,
    /** Validating, indexing and publishing the store. */
    INDEXING
}
