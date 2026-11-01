package com.example.refdata.server.snapshot;

import com.example.refdata.contract.Digests;
import com.example.refdata.model.EntityKind;
import com.example.refdata.protocol.v1.Codec;
import com.example.refdata.protocol.v1.EntityManifest;
import com.example.refdata.protocol.v1.SnapshotInfo;
import com.google.protobuf.ByteString;

import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The encoded, immutable snapshot that the server publishes for the life of the process: every chunk frame per
 * codec, the manifest, the hashes and the content-derived id. Thread-safe; nothing here is ever modified after
 * construction.
 */
public final class Snapshot {
    private final SnapshotInfo info;
    private final byte[] snapshotId;
    private final List<EncodedChunk> chunks;
    private final Map<EntityKind, EntityManifest> manifests;
    private final Map<EntityKind, byte[]> entityHashes;
    private final Set<Codec> codecs;
    private final int largestFrameBytes;
    private final Map<Codec, Integer> largestFrameByCodec;

    Snapshot(SnapshotInfo info, List<EncodedChunk> chunks, Map<EntityKind, EntityManifest> manifests,
             Map<EntityKind, byte[]> entityHashes, Set<Codec> codecs) {
        this.info = info;
        this.snapshotId = info.getSnapshotId().toByteArray();
        this.chunks = List.copyOf(chunks);
        this.manifests = Collections.unmodifiableMap(new EnumMap<>(manifests));
        this.entityHashes = Collections.unmodifiableMap(new EnumMap<>(entityHashes));
        this.codecs = Collections.unmodifiableSet(codecs);
        Map<Codec, Integer> perCodec = new EnumMap<>(Codec.class);
        for (Codec codec : codecs) perCodec.put(codec, 0);
        for (EncodedChunk c : chunks) for (Codec codec : c.codecs()) perCodec.merge(codec, c.frameBytes(codec), Math::max);
        this.largestFrameByCodec = Collections.unmodifiableMap(perCodec);
        int largest = 0;
        for (int v : perCodec.values()) largest = Math.max(largest, v);
        this.largestFrameBytes = largest;
    }

    /** The SnapshotInfo sent in HelloAck and SnapshotBegin. */
    public SnapshotInfo info() { return info; }
    /** 16-byte content-derived id. */
    public byte[] snapshotId() { return snapshotId.clone(); }
    public ByteString snapshotIdBytes() { return info.getSnapshotId(); }
    public String snapshotIdHex() { return Digests.hex(snapshotId); }
    public byte[] contentSha256() { return info.getContentSha256().toByteArray(); }
    public long dataVersion() { return info.getDataVersion(); }
    /** True when the id equals this snapshot's id. */
    public boolean hasId(ByteString id) { return info.getSnapshotId().equals(id); }

    public int chunkCount() { return chunks.size(); }
    public EncodedChunk chunk(int seq) { return chunks.get(seq); }
    public List<EncodedChunk> chunks() { return chunks; }
    /** Manifest entry per kind, in stream order (all six kinds, even with zero rows). */
    public Map<EntityKind, EntityManifest> manifests() { return manifests; }
    public EntityManifest manifest(EntityKind kind) { return manifests.get(kind); }
    /** SHA-256 over the uncompressed payloads of the kind, in seq order. */
    public byte[] entityHash(EntityKind kind) { return entityHashes.get(kind).clone(); }
    /** The codecs every chunk is encoded for (NONE plus the configured extras). */
    public Set<Codec> codecs() { return codecs; }
    /** True when chunks are pre-encoded for the codec. */
    public boolean supports(Codec codec) { return codecs.contains(codec); }
    /** The largest frame of any chunk in any codec: a client must accept at least this. */
    public int largestFrameBytes() { return largestFrameBytes; }
    /** The largest chunk frame for one codec (what a client choosing that codec must accept). */
    public int largestFrameBytes(Codec codec) {
        Integer v = largestFrameByCodec.get(codec);
        if (v == null) throw new IllegalArgumentException("snapshot is not encoded for " + codec);
        return v;
    }
    public long totalRecords() { return info.getTotalRecords(); }
    public long totalUncompressedBytes() { return info.getTotalUncompressedBytes(); }

    /** Sum of frame bytes over all chunks for a codec (what a full unfiltered transfer sends). */
    public long totalFrameBytes(Codec codec) {
        long t = 0;
        for (EncodedChunk c : chunks) t += c.frameBytes(codec);
        return t;
    }

    /** Re-checks the stored CRC of every chunk in every codec; returns the seq of the first corrupt chunk, or -1. */
    public int verifyAll() {
        for (EncodedChunk c : chunks) for (Codec codec : c.codecs()) if (!c.verifyCrc(codec)) return c.seq();
        return -1;
    }
}
