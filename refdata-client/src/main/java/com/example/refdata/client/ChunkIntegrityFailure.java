package com.example.refdata.client;

/** A chunk failed its seq, CRC or record-count check (design row C-3): reported to the server, then resumed elsewhere. */
final class ChunkIntegrityFailure extends Exception {
    private final int seq;

    ChunkIntegrityFailure(int seq, String message) {
        super("chunk " + seq + ": " + message);
        this.seq = seq;
    }

    int seq() { return seq; }
}
