package com.example.refdata.client;

import com.example.refdata.contract.Digests;
import com.example.refdata.contract.EntityCodec;
import com.example.refdata.model.EntityKind;
import com.example.refdata.model.RefDataStoreBuilder;
import com.example.refdata.model.validation.ValidationPolicy;
import com.example.refdata.protocol.v1.EntityManifest;
import com.example.refdata.protocol.v1.SnapshotChunk;
import com.example.refdata.protocol.v1.SnapshotEnd;
import com.example.refdata.protocol.v1.SnapshotInfo;
import com.example.refdata.schema.v1.AccountBatch;
import com.example.refdata.schema.v1.AddressBatch;
import com.example.refdata.schema.v1.ClientBatch;
import com.example.refdata.schema.v1.CountryBatch;
import com.example.refdata.schema.v1.CurrencyBatch;
import com.example.refdata.schema.v1.SettlementInstructionBatch;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The staging area of one snapshot transfer: the model records decoded so far, the running per-entity hashes and
 * counts, and the position in the chunk sequence. It survives reconnects so that a transfer can be resumed
 * (design section 9.8) and performs the consumer-side checks of section 9.7.
 */
final class SnapshotStaging {
    private static final Logger log = LoggerFactory.getLogger(SnapshotStaging.class);

    /** Running state of one entity type in the transfer. */
    private static final class TypeProgress {
        final EntityManifest manifest;
        final EntityKind kind;                // null when this build does not know the type
        final MessageDigest digest;
        long records;
        int chunks;

        TypeProgress(EntityManifest manifest) {
            this.manifest = manifest;
            this.kind = EntityKind.fromWire(manifest.getEntityType());
            this.digest = kind == null ? null : Digests.sha256();
        }
    }

    private final SnapshotInfo info;
    private final RefDataStoreBuilder builder;
    private final Map<Integer, TypeProgress> progressByType = new HashMap<>();
    private final int[] expectedSeqs;        // every seq this transfer must deliver, in order
    private final int[] expectedTypes;       // the entity type of each expected seq
    private int position;                    // index into expectedSeqs of the next chunk to accept
    private long acceptedChunks;
    private long acceptedRecords;
    private int skippedUnknownTypeChunks;
    private int failedSeq = -1;
    private int failuresAtSeq;
    private final Set<Integer> unknownTypesLogged = new HashSet<>();

    /**
     * @param info      the snapshot as announced in SnapshotBegin
     * @param sentTypes the entity types the server will send (SnapshotBegin.entity_types), in any order
     */
    SnapshotStaging(SnapshotInfo info, List<Integer> sentTypes, ValidationPolicy policy) {
        this.info = info;
        Set<Integer> wanted = new HashSet<>(sentTypes);
        Set<EntityKind> loadedKinds = EnumSet.noneOf(EntityKind.class);
        for (int type : wanted) {
            EntityKind kind = EntityKind.fromWire(type);
            if (kind != null) loadedKinds.add(kind);
        }
        this.builder = new RefDataStoreBuilder().policy(policy).dataVersion(info.getDataVersion()).loadedKinds(loadedKinds);
        List<Integer> seqs = new ArrayList<>();
        List<Integer> types = new ArrayList<>();
        for (EntityManifest m : info.getEntitiesList()) {
            if (!wanted.contains(m.getEntityType())) continue;
            TypeProgress p = new TypeProgress(m);
            progressByType.put(m.getEntityType(), p);
            if (p.kind != null) builder.expect(p.kind, m.getRecordCount());
            for (int i = 0; i < m.getChunkCount(); i++) {
                seqs.add(m.getFirstSeq() + i);
                types.add(m.getEntityType());
            }
        }
        this.expectedSeqs = seqs.stream().mapToInt(Integer::intValue).toArray();
        this.expectedTypes = types.stream().mapToInt(Integer::intValue).toArray();
    }

    SnapshotInfo info() { return info; }
    byte[] snapshotId() { return info.getSnapshotId().toByteArray(); }
    boolean complete() { return position == expectedSeqs.length; }
    /**
     * The first seq not yet accepted: the resume point. Once every chunk is in (only SnapshotEnd is missing) this is
     * one past the last expected seq, which the server answers with an empty resume: SnapshotBegin then SnapshotEnd.
     */
    int nextSeq() {
        if (!complete()) return expectedSeqs[position];
        return expectedSeqs.length == 0 ? 0 : expectedSeqs[expectedSeqs.length - 1] + 1;
    }
    long acceptedChunks() { return acceptedChunks; }
    long acceptedRecords() { return acceptedRecords; }
    int skippedUnknownTypeChunks() { return skippedUnknownTypeChunks; }
    int expectedChunks() { return expectedSeqs.length; }

    /** Records a rejection of the chunk at {@link #nextSeq()} and returns how often that same chunk has now failed. */
    int recordFailure() {
        int seq = nextSeq();
        if (seq != failedSeq) {
            failedSeq = seq;
            failuresAtSeq = 0;
        }
        return ++failuresAtSeq;
    }

    /**
     * Checks one chunk and stages its records. Contents of unknown entity types are skipped.
     * @return the uncompressed payload size (0 for a skipped chunk)
     * @throws ChunkIntegrityFailure on a seq, CRC or record-count mismatch (the chunk is not staged; resume from it)
     * @throws SnapshotCorruption    on a malformed batch or a per-entity hash/count mismatch
     */
    int accept(SnapshotChunk chunk) throws ChunkIntegrityFailure, SnapshotCorruption {
        if (complete()) throw new ChunkIntegrityFailure(chunk.getSeq(), "no more chunks expected");
        int seq = nextSeq();
        if (chunk.getSeq() != seq) throw new ChunkIntegrityFailure(seq, "expected seq " + seq + " but got " + chunk.getSeq());
        if (chunk.getEntityType() != expectedTypes[position]) {
            throw new ChunkIntegrityFailure(seq, "expected entity type " + expectedTypes[position] + " but got " + chunk.getEntityType());
        }
        byte[] payload = chunk.getPayload().toByteArray();
        int crc = Digests.crc32(payload);
        if (crc != chunk.getCrc32()) {
            throw new ChunkIntegrityFailure(seq, "CRC mismatch: computed " + Integer.toHexString(crc) + ", header " + Integer.toHexString(chunk.getCrc32()));
        }
        TypeProgress p = progressByType.get(chunk.getEntityType());
        int uncompressedBytes = 0;
        if (p.kind == null) {
            if (unknownTypesLogged.add(chunk.getEntityType())) {
                log.info("skipping chunks of unknown entity type {} ({} records in the manifest)", chunk.getEntityType(), p.manifest.getRecordCount());
            }
            skippedUnknownTypeChunks++;
        } else {
            byte[] batch;
            try {
                batch = Payloads.decompress(chunk.getCodec(), payload, chunk.getUncompressedSize());
            } catch (IllegalArgumentException e) {
                throw new SnapshotCorruption("chunk " + seq + " cannot be decompressed: " + e.getMessage(), e);
            }
            int decoded;
            try {
                decoded = stage(p.kind, batch, chunk.getRecordCount());
            } catch (InvalidProtocolBufferException e) {
                throw new SnapshotCorruption("chunk " + seq + " holds a malformed " + p.kind + " batch", e);
            }
            if (decoded != chunk.getRecordCount()) {
                throw new ChunkIntegrityFailure(seq, "decoded " + decoded + " records but header says " + chunk.getRecordCount());
            }
            p.digest.update(batch);
            uncompressedBytes = batch.length;
        }
        p.records += chunk.getRecordCount();
        p.chunks++;
        acceptedChunks++;
        acceptedRecords += chunk.getRecordCount();
        position++;
        if (p.chunks == p.manifest.getChunkCount()) verifyTypeComplete(p);
        return uncompressedBytes;
    }

    private void verifyTypeComplete(TypeProgress p) throws SnapshotCorruption {
        String name = p.kind == null ? "type " + p.manifest.getEntityType() : p.kind.name();
        if (p.records != p.manifest.getRecordCount()) {
            throw new SnapshotCorruption(name + ": received " + p.records + " records, manifest says " + p.manifest.getRecordCount());
        }
        if (p.kind == null) return;
        byte[] actual = p.digest.digest();
        byte[] expected = p.manifest.getEntitySha256().toByteArray();
        if (!Arrays.equals(actual, expected)) {
            throw new SnapshotCorruption(name + ": SHA-256 " + Digests.hex(actual) + " does not match the manifest's " + Digests.hex(expected));
        }
        log.debug("{}: {} records in {} chunks verified", name, p.records, p.chunks);
    }

    /**
     * Decodes a batch and, only when it holds exactly {@code expectedCount} records, stages them in the builder (so a
     * rejected chunk is never half-applied). The protobuf objects are dropped at once.
     * @return the number of records the batch holds
     */
    private int stage(EntityKind kind, byte[] batch, int expectedCount) throws InvalidProtocolBufferException {
        switch (kind) {
            case COUNTRY -> {
                CountryBatch b = CountryBatch.parseFrom(batch);
                if (b.getItemsCount() == expectedCount) for (var item : b.getItemsList()) builder.country(EntityCodec.fromProto(item));
                return b.getItemsCount();
            }
            case CURRENCY -> {
                CurrencyBatch b = CurrencyBatch.parseFrom(batch);
                if (b.getItemsCount() == expectedCount) for (var item : b.getItemsList()) builder.currency(EntityCodec.fromProto(item));
                return b.getItemsCount();
            }
            case CLIENT -> {
                ClientBatch b = ClientBatch.parseFrom(batch);
                if (b.getItemsCount() == expectedCount) for (var item : b.getItemsList()) builder.client(EntityCodec.fromProto(item));
                return b.getItemsCount();
            }
            case ADDRESS -> {
                AddressBatch b = AddressBatch.parseFrom(batch);
                if (b.getItemsCount() == expectedCount) for (var item : b.getItemsList()) builder.address(EntityCodec.fromProto(item));
                return b.getItemsCount();
            }
            case ACCOUNT -> {
                AccountBatch b = AccountBatch.parseFrom(batch);
                if (b.getItemsCount() == expectedCount) for (var item : b.getItemsList()) builder.account(EntityCodec.fromProto(item));
                return b.getItemsCount();
            }
            case SETTLEMENT_INSTRUCTION -> {
                SettlementInstructionBatch b = SettlementInstructionBatch.parseFrom(batch);
                if (b.getItemsCount() == expectedCount) for (var item : b.getItemsList()) builder.settlementInstruction(EntityCodec.fromProto(item));
                return b.getItemsCount();
            }
            default -> throw new IllegalStateException("unhandled kind " + kind);
        }
    }

    /**
     * The SnapshotEnd checks: every expected chunk accepted, the client's own totals against the manifest and the
     * server's totals, and (for unfiltered transfers only) the whole-snapshot hash.
     */
    void verifyEnd(SnapshotEnd end, boolean unfiltered) throws SnapshotCorruption {
        if (!complete()) {
            throw new SnapshotCorruption("SnapshotEnd after " + position + " of " + expectedSeqs.length + " expected chunks");
        }
        long manifestRecords = 0;
        for (TypeProgress p : progressByType.values()) manifestRecords += p.manifest.getRecordCount();
        if (acceptedRecords != manifestRecords) {
            throw new SnapshotCorruption("accepted " + acceptedRecords + " records, manifest totals " + manifestRecords);
        }
        if (end.getChunksSent() != acceptedChunks || end.getRecordsSent() != acceptedRecords) {
            throw new SnapshotCorruption("SnapshotEnd reports " + end.getChunksSent() + " chunks / " + end.getRecordsSent()
                    + " records, client accepted " + acceptedChunks + " / " + acceptedRecords);
        }
        if (!unfiltered) return;
        List<byte[]> entityHashes = new ArrayList<>();
        for (EntityManifest m : info.getEntitiesList()) entityHashes.add(m.getEntitySha256().toByteArray());
        byte[] computed = Digests.contentSha256(entityHashes);
        byte[] fromEnd = end.getContentSha256().toByteArray();
        byte[] fromInfo = info.getContentSha256().toByteArray();
        if (!Arrays.equals(computed, fromEnd) || !Arrays.equals(computed, fromInfo)) {
            throw new SnapshotCorruption("whole-snapshot SHA-256 mismatch: manifest gives " + Digests.hex(computed)
                    + ", SnapshotEnd " + Digests.hex(fromEnd) + ", SnapshotInfo " + Digests.hex(fromInfo));
        }
    }

    /** The builder holding the staged records; call only after {@link #verifyEnd}. */
    RefDataStoreBuilder builder() { return builder; }
}
