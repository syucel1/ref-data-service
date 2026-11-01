package com.example.refdata.server.snapshot;

import com.example.refdata.contract.Digests;
import com.example.refdata.contract.EntityCodec;
import com.example.refdata.contract.Framing;
import com.example.refdata.model.EntityKind;
import com.example.refdata.model.RefDataStore;
import com.example.refdata.protocol.v1.Codec;
import com.example.refdata.protocol.v1.EntityManifest;
import com.example.refdata.protocol.v1.Envelope;
import com.example.refdata.protocol.v1.SnapshotChunk;
import com.example.refdata.protocol.v1.SnapshotInfo;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.MessageLite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Turns a {@link RefDataStore} into a {@link Snapshot}: one entity kind per chunk, primary-key order, kinds in
 * {@link EntityKind} order, chunks cut at the configured uncompressed target and never letting a frame exceed the
 * frame limit, CRC-32 per payload as sent, SHA-256 per kind over the uncompressed payloads, the content hash and the
 * content-derived snapshot id. Encoding the same store with the same settings gives byte-identical output.
 */
public final class SnapshotEncoder {
    private static final Logger log = LoggerFactory.getLogger(SnapshotEncoder.class);

    /** Field number of {@code items} in every {@code <Entity>Batch} message. */
    private static final int BATCH_ITEMS_FIELD = 1;
    /** Upper bound of the frame bytes around a payload: length prefix, Envelope tag+length, six scalar fields, payload tag+length. */
    private static final int FRAME_OVERHEAD_BOUND = 4 + 6 + 6 * 6 + 6;

    private final int chunkTargetBytes;
    private final int frameMaxBytes;
    private final Set<Codec> codecs;

    /**
     * @param chunkTargetBytes uncompressed payload size at which a chunk is closed
     * @param frameMaxBytes    largest frame the server may send; must be at least the target plus {@link Framing#CHUNK_HEADROOM_BYTES}
     * @param extraCodecs      codecs to pre-encode besides NONE
     */
    public SnapshotEncoder(int chunkTargetBytes, int frameMaxBytes, Set<Codec> extraCodecs) {
        if ((long) chunkTargetBytes + Framing.CHUNK_HEADROOM_BYTES > frameMaxBytes) {
            throw new IllegalArgumentException("chunk target " + chunkTargetBytes + " + headroom " + Framing.CHUNK_HEADROOM_BYTES + " exceeds frame limit " + frameMaxBytes);
        }
        this.chunkTargetBytes = chunkTargetBytes;
        this.frameMaxBytes = frameMaxBytes;
        Set<Codec> all = new LinkedHashSet<>();
        all.add(Codec.CODEC_NONE);
        for (Codec c : extraCodecs) {
            if (c == Codec.CODEC_UNSPECIFIED || c == Codec.UNRECOGNIZED) throw new IllegalArgumentException("cannot pre-encode codec " + c);
            all.add(c);
        }
        this.codecs = all;
    }

    /**
     * Encodes the store.
     *
     * @param source description of where the data came from (no credentials), carried in SnapshotInfo
     * @param builtAtEpochMs timestamp carried in SnapshotInfo (not part of the id)
     * @throws IllegalStateException when a single record does not fit into a frame (design S-11)
     */
    public Snapshot encode(RefDataStore store, String source, long builtAtEpochMs) {
        long start = System.nanoTime();
        List<EncodedChunk> chunks = new ArrayList<>();
        Map<EntityKind, EntityManifest> manifests = new EnumMap<>(EntityKind.class);
        Map<EntityKind, byte[]> hashes = new EnumMap<>(EntityKind.class);
        long totalRecords = 0, totalBytes = 0;
        for (EntityKind kind : EntityKind.values()) {
            int firstSeq = chunks.size();
            MessageDigest sha = Digests.sha256();
            long kindBytes = 0;
            List<EncodedChunk> kindChunks = chunkKind(store, kind, firstSeq, sha);
            for (EncodedChunk c : kindChunks) kindBytes += c.uncompressedSize();
            chunks.addAll(kindChunks);
            byte[] hash = sha.digest();
            hashes.put(kind, hash);
            long records = store.count(kind);
            manifests.put(kind, EntityManifest.newBuilder().setEntityType(kind.wireCode()).setRecordCount(records).setFirstSeq(firstSeq)
                    .setChunkCount(kindChunks.size()).setUncompressedBytes(kindBytes).setEntitySha256(ByteString.copyFrom(hash)).build());
            totalRecords += records;
            totalBytes += kindBytes;
        }
        List<byte[]> hashList = new ArrayList<>();
        for (EntityKind kind : EntityKind.values()) hashList.add(hashes.get(kind));
        byte[] content = Digests.contentSha256(hashList);
        int[] types = new int[chunks.size()], counts = new int[chunks.size()];
        for (int i = 0; i < chunks.size(); i++) { types[i] = chunks.get(i).kind().wireCode(); counts[i] = chunks.get(i).recordCount(); }
        byte[] id = Digests.snapshotId(Framing.SCHEMA_MAJOR, Framing.SCHEMA_MINOR, types, counts, content);

        SnapshotInfo.Builder info = SnapshotInfo.newBuilder().setSnapshotId(ByteString.copyFrom(id)).setDataVersion(store.dataVersion())
                .setSchemaMajor(Framing.SCHEMA_MAJOR).setSchemaMinor(Framing.SCHEMA_MINOR).setBuiltAtEpochMs(builtAtEpochMs)
                .setSource(source == null ? "" : source).setContentSha256(ByteString.copyFrom(content))
                .setTotalChunks(chunks.size()).setTotalRecords(totalRecords).setTotalUncompressedBytes(totalBytes);
        for (EntityKind kind : EntityKind.values()) info.addEntities(manifests.get(kind));
        Snapshot snapshot = new Snapshot(info.build(), chunks, manifests, hashes, codecs);
        log.info("encoded snapshot {}: {} chunks, {} records, {} uncompressed bytes, codecs {}, largest frame {} bytes, {} ms",
                snapshot.snapshotIdHex(), chunks.size(), totalRecords, totalBytes, codecs, snapshot.largestFrameBytes(), (System.nanoTime() - start) / 1_000_000);
        return snapshot;
    }

    private List<EncodedChunk> chunkKind(RefDataStore store, EntityKind kind, int firstSeq, MessageDigest sha) {
        return switch (kind) {
            case COUNTRY -> chunkRows(store.countries(), EntityCodec::toProto, kind, firstSeq, sha);
            case CURRENCY -> chunkRows(store.currencies(), EntityCodec::toProto, kind, firstSeq, sha);
            case CLIENT -> chunkRows(store.clients(), EntityCodec::toProto, kind, firstSeq, sha);
            case ADDRESS -> chunkRows(store.addresses(), EntityCodec::toProto, kind, firstSeq, sha);
            case ACCOUNT -> chunkRows(store.accounts(), EntityCodec::toProto, kind, firstSeq, sha);
            case SETTLEMENT_INSTRUCTION -> chunkRows(store.settlementInstructions(), EntityCodec::toProto, kind, firstSeq, sha);
        };
    }

    /**
     * Serializes the rows as the {@code items} of one batch per chunk. Writing each item as a length-delimited field 1 is
     * exactly what serializing an {@code <Entity>Batch} does, so concatenated chunk payloads of one kind equal one big batch.
     */
    private <R, P extends MessageLite> List<EncodedChunk> chunkRows(List<R> rows, Function<R, P> toProto, EntityKind kind, int firstSeq, MessageDigest sha) {
        List<EncodedChunk> out = new ArrayList<>();
        ByteArrayOutputStream payload = new ByteArrayOutputStream(Math.min(chunkTargetBytes + (64 << 10), 1 << 20));
        int records = 0;
        long position = 0;
        try {
            CodedOutputStream cos = CodedOutputStream.newInstance(payload);
            for (R row : rows) {
                P msg = toProto.apply(row);
                int itemBytes = CodedOutputStream.computeMessageSize(BATCH_ITEMS_FIELD, msg);
                if (itemBytes + FRAME_OVERHEAD_BOUND > frameMaxBytes) {
                    throw new IllegalStateException(kind + " record at position " + position + " needs " + itemBytes + " bytes, more than the frame limit " + frameMaxBytes + " allows");
                }
                boolean overTarget = records > 0 && payload.size() + itemBytes > chunkTargetBytes;
                boolean overFrame = records > 0 && payload.size() + itemBytes + FRAME_OVERHEAD_BOUND > frameMaxBytes;
                if (overTarget || overFrame) {
                    cos.flush();
                    out.add(close(kind, firstSeq + out.size(), records, payload.toByteArray(), sha));
                    payload.reset();
                    records = 0;
                    cos = CodedOutputStream.newInstance(payload);
                }
                cos.writeMessage(BATCH_ITEMS_FIELD, msg);
                cos.flush();
                records++;
                position++;
            }
            if (records > 0) out.add(close(kind, firstSeq + out.size(), records, payload.toByteArray(), sha));
        } catch (IOException e) {
            throw new IllegalStateException("in-memory serialization failed", e);
        }
        return out;
    }

    private EncodedChunk close(EntityKind kind, int seq, int records, byte[] uncompressed, MessageDigest sha) {
        sha.update(uncompressed);
        Map<Codec, byte[]> frames = new EnumMap<>(Codec.class);
        Map<Codec, Integer> crcs = new EnumMap<>(Codec.class);
        Map<Codec, Integer> offsets = new EnumMap<>(Codec.class);
        for (Codec codec : codecs) {
            byte[] payload = PayloadCodecs.compress(codec, uncompressed);
            int crc = Digests.crc32(payload);
            SnapshotChunk chunk = SnapshotChunk.newBuilder().setSeq(seq).setEntityType(kind.wireCode()).setRecordCount(records)
                    .setCodec(codec).setUncompressedSize(uncompressed.length).setCrc32(crc).setPayload(ByteString.copyFrom(payload)).build();
            byte[] frame = frame(chunk);
            if (frame.length > frameMaxBytes) {
                throw new IllegalStateException(kind + " chunk " + seq + " frame is " + frame.length + " bytes, more than the frame limit " + frameMaxBytes);
            }
            frames.put(codec, frame);
            crcs.put(codec, crc);
            offsets.put(codec, frame.length - payload.length);
        }
        return new EncodedChunk(seq, kind, records, uncompressed.length, frames, crcs, offsets);
    }

    /**
     * Length prefix + Envelope{snapshot_chunk}. Protobuf writes fields in field-number order and {@code payload} is the
     * highest-numbered field of SnapshotChunk, so the payload bytes are exactly the tail of the frame (which is what
     * {@link EncodedChunk#verifyCrc} relies on).
     */
    private static byte[] frame(SnapshotChunk chunk) {
        Envelope env = Envelope.newBuilder().setSnapshotChunk(chunk).build();
        int size = env.getSerializedSize();
        byte[] frame = new byte[Framing.LENGTH_PREFIX_BYTES + size];
        frame[0] = (byte) (size >>> 24); frame[1] = (byte) (size >>> 16); frame[2] = (byte) (size >>> 8); frame[3] = (byte) size;
        try {
            CodedOutputStream cos = CodedOutputStream.newInstance(frame, Framing.LENGTH_PREFIX_BYTES, size);
            env.writeTo(cos);
            cos.checkNoSpaceLeft();
        } catch (IOException e) {
            throw new IllegalStateException("in-memory serialization failed", e);
        }
        return frame;
    }
}
