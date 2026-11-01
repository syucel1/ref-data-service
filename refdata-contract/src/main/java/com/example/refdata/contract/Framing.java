package com.example.refdata.contract;

/**
 * Constants of the wire contract that are not expressible in the .proto files (design sections 9.2 and 11).
 * Frame = 4-byte big-endian unsigned length (excluding itself) + one {@code Envelope}.
 */
public final class Framing {
    private Framing() {}

    public static final int PROTOCOL_MAJOR = 1;
    public static final int PROTOCOL_MINOR = 0;
    public static final int SCHEMA_MAJOR = 1;
    public static final int SCHEMA_MINOR = 0;

    public static final int LENGTH_PREFIX_BYTES = 4;
    /** Every client must accept frames of at least this size; a Hello with max_frame_bytes = 0 means this. */
    public static final int DEFAULT_MAX_FRAME_BYTES = 1 << 20;
    /** The server only ever receives small control messages; anything larger is a protocol violation. */
    public static final int SERVER_INBOUND_MAX_FRAME_BYTES = 64 << 10;
    /** A chunk target may not come closer than this to the frame limit (room for the chunk header and one record). */
    public static final int CHUNK_HEADROOM_BYTES = 64 << 10;
    /** Heartbeats missed before a connection is considered dead. */
    public static final int HEARTBEAT_MISSES = 3;
}
