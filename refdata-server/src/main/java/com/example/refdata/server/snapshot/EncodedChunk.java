package com.example.refdata.server.snapshot;

import com.example.refdata.contract.Digests;
import com.example.refdata.model.EntityKind;
import com.example.refdata.protocol.v1.Codec;
import com.example.refdata.protocol.v1.Envelope;
import com.example.refdata.protocol.v1.SnapshotChunk;
import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Set;

/**
 * One chunk of the snapshot, pre-encoded as a complete frame (length prefix + Envelope{SnapshotChunk}) once per codec.
 * The frame bytes are immutable; each is wrapped once as a read-only, unreleasable Netty buffer and handed out as
 * a {@link ByteBuf#retainedDuplicate() duplicate} so that no session can disturb the shared bytes.
 */
public final class EncodedChunk {
    private final int seq;
    private final EntityKind kind;
    private final int recordCount;
    private final int uncompressedSize;
    private final Map<Codec, byte[]> frames;
    private final Map<Codec, ByteBuf> buffers;
    private final Map<Codec, Integer> crcs;
    private final Map<Codec, Integer> payloadOffsets;

    EncodedChunk(int seq, EntityKind kind, int recordCount, int uncompressedSize, Map<Codec, byte[]> frames,
                 Map<Codec, Integer> crcs, Map<Codec, Integer> payloadOffsets) {
        this.seq = seq;
        this.kind = kind;
        this.recordCount = recordCount;
        this.uncompressedSize = uncompressedSize;
        this.frames = Collections.unmodifiableMap(new EnumMap<>(frames));
        this.crcs = Collections.unmodifiableMap(new EnumMap<>(crcs));
        this.payloadOffsets = Collections.unmodifiableMap(new EnumMap<>(payloadOffsets));
        Map<Codec, ByteBuf> bufs = new EnumMap<>(Codec.class);
        for (Map.Entry<Codec, byte[]> e : frames.entrySet()) {
            bufs.put(e.getKey(), Unpooled.unreleasableBuffer(Unpooled.wrappedBuffer(e.getValue()).asReadOnly()));
        }
        this.buffers = Collections.unmodifiableMap(bufs);
    }

    /** Position of the chunk in the full snapshot, from 0. */
    public int seq() { return seq; }
    public EntityKind kind() { return kind; }
    public int recordCount() { return recordCount; }
    /** Size of the serialized batch before compression. */
    public int uncompressedSize() { return uncompressedSize; }
    /** The codecs this chunk was encoded for (always includes NONE). */
    public Set<Codec> codecs() { return frames.keySet(); }

    /** Whole frame length (prefix included) for a codec. */
    public int frameBytes(Codec codec) { return frame(codec).length; }

    /** The immutable frame bytes (callers must not modify them). */
    public byte[] frame(Codec codec) {
        byte[] f = frames.get(codec);
        if (f == null) throw new IllegalArgumentException("chunk " + seq + " is not encoded for " + codec);
        return f;
    }

    /** A fresh duplicate of the shared read-only buffer, safe to write to a channel; releasing it is a no-op. */
    public ByteBuf frameBuffer(Codec codec) {
        ByteBuf b = buffers.get(codec);
        if (b == null) throw new IllegalArgumentException("chunk " + seq + " is not encoded for " + codec);
        return b.retainedDuplicate();
    }

    /** CRC-32 stored in the frame for the codec (over the payload as sent). */
    public int crc32(Codec codec) { return crcs.get(codec); }

    /**
     * Re-computes the CRC-32 of the payload bytes inside the stored frame and compares it with the value the frame
     * carries (design S-16 / P-18). Returns true when the stored bytes are intact.
     */
    public boolean verifyCrc(Codec codec) {
        byte[] f = frame(codec);
        int off = payloadOffsets.get(codec);
        int len = f.length - off;
        return Digests.crc32(f, off, len) == crcs.get(codec);
    }

    /** Decodes the stored frame back into its message (tests and diagnostics). */
    public SnapshotChunk decode(Codec codec) {
        byte[] f = frame(codec);
        try {
            return Envelope.parseFrom(Arrays.copyOfRange(f, 4, f.length)).getSnapshotChunk();
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("stored frame " + seq + " does not parse", e);
        }
    }
}
