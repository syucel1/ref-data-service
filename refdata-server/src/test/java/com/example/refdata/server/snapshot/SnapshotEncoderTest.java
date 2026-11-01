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
import com.example.refdata.schema.v1.AccountBatch;
import com.example.refdata.schema.v1.AddressBatch;
import com.example.refdata.schema.v1.ClientBatch;
import com.example.refdata.schema.v1.CountryBatch;
import com.example.refdata.schema.v1.CurrencyBatch;
import com.example.refdata.schema.v1.SettlementInstructionBatch;
import com.example.refdata.server.TestStores;
import io.netty.buffer.ByteBuf;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class SnapshotEncoderTest {
    private static final int TARGET = 2048;
    private static final int FRAME_MAX = TARGET + Framing.CHUNK_HEADROOM_BYTES;
    private static final Set<Codec> ALL = EnumSet.of(Codec.CODEC_LZ4, Codec.CODEC_ZSTD);

    private static Snapshot encode(RefDataStore store, Set<Codec> extra) {
        return new SnapshotEncoder(TARGET, FRAME_MAX, extra).encode(store, "test", 1_700_000_000_000L);
    }

    @Test public void chunkBoundariesRespectTargetAndFrameLimit() {
        Snapshot s = encode(TestStores.sized(200), ALL);
        assertTrue("expect several chunks", s.chunkCount() > 6);
        for (EncodedChunk c : s.chunks()) {
            assertTrue(c.uncompressedSize() <= TARGET || c.recordCount() == 1);
            for (Codec codec : s.codecs()) assertTrue(c.frameBytes(codec) <= FRAME_MAX);
            int next = c.seq() + 1;
            if (next < s.chunkCount() && s.chunk(next).kind() == c.kind()) {
                // the chunk was closed only because the next record would not fit
                SnapshotChunk nextChunk = s.chunk(next).decode(Codec.CODEC_NONE);
                int firstItem = firstItemSize(nextChunk.getPayload().toByteArray());
                assertTrue(c.uncompressedSize() + firstItem > TARGET);
            }
        }
        assertTrue(s.largestFrameBytes() <= FRAME_MAX);
    }

    @Test public void seqContiguousAndManifestConsistent() {
        Snapshot s = encode(TestStores.sized(200), Set.of());
        for (int i = 0; i < s.chunkCount(); i++) assertEquals(i, s.chunk(i).seq());
        int expectFirst = 0;
        long records = 0, bytes = 0;
        for (EntityKind k : EntityKind.values()) {
            EntityManifest m = s.manifest(k);
            assertEquals(k.wireCode(), m.getEntityType());
            assertEquals(expectFirst, m.getFirstSeq());
            long kindRecords = 0, kindBytes = 0;
            for (int i = 0; i < m.getChunkCount(); i++) {
                EncodedChunk c = s.chunk(m.getFirstSeq() + i);
                assertEquals(k, c.kind());
                kindRecords += c.recordCount();
                kindBytes += c.uncompressedSize();
            }
            assertEquals(m.getRecordCount(), kindRecords);
            assertEquals(m.getUncompressedBytes(), kindBytes);
            expectFirst += m.getChunkCount();
            records += kindRecords;
            bytes += kindBytes;
        }
        assertEquals(s.chunkCount(), expectFirst);
        assertEquals(s.chunkCount(), s.info().getTotalChunks());
        assertEquals(records, s.info().getTotalRecords());
        assertEquals(bytes, s.info().getTotalUncompressedBytes());
        assertEquals(42, s.dataVersion());
        assertEquals(EntityKind.values().length, s.info().getEntitiesCount());
    }

    @Test public void decodingEveryChunkReproducesTheStore() throws Exception {
        RefDataStore store = TestStores.sized(150);
        Snapshot s = encode(store, Set.of());
        assertEquals(store.countries(), decodeAll(s, EntityKind.COUNTRY, p -> CountryBatch.parseFrom(p).getItemsList().stream().map(EntityCodec::fromProto).toList()));
        assertEquals(store.currencies(), decodeAll(s, EntityKind.CURRENCY, p -> CurrencyBatch.parseFrom(p).getItemsList().stream().map(EntityCodec::fromProto).toList()));
        assertEquals(store.clients(), decodeAll(s, EntityKind.CLIENT, p -> ClientBatch.parseFrom(p).getItemsList().stream().map(EntityCodec::fromProto).toList()));
        assertEquals(store.addresses(), decodeAll(s, EntityKind.ADDRESS, p -> AddressBatch.parseFrom(p).getItemsList().stream().map(EntityCodec::fromProto).toList()));
        assertEquals(store.accounts(), decodeAll(s, EntityKind.ACCOUNT, p -> AccountBatch.parseFrom(p).getItemsList().stream().map(EntityCodec::fromProto).toList()));
        assertEquals(store.settlementInstructions(), decodeAll(s, EntityKind.SETTLEMENT_INSTRUCTION, p -> SettlementInstructionBatch.parseFrom(p).getItemsList().stream().map(EntityCodec::fromProto).toList()));
    }

    @Test public void chunkHeaderFieldsAndCrcMatchPayload() throws Exception {
        Snapshot s = encode(TestStores.sized(50), ALL);
        for (EncodedChunk c : s.chunks()) {
            for (Codec codec : s.codecs()) {
                byte[] frame = c.frame(codec);
                int len = ((frame[0] & 0xff) << 24) | ((frame[1] & 0xff) << 16) | ((frame[2] & 0xff) << 8) | (frame[3] & 0xff);
                assertEquals(frame.length - 4, len);
                Envelope env = Envelope.parseFrom(Arrays.copyOfRange(frame, 4, frame.length));
                assertEquals(Envelope.BodyCase.SNAPSHOT_CHUNK, env.getBodyCase());
                SnapshotChunk chunk = env.getSnapshotChunk();
                assertEquals(c.seq(), chunk.getSeq());
                assertEquals(c.kind().wireCode(), chunk.getEntityType());
                assertEquals(c.recordCount(), chunk.getRecordCount());
                assertEquals(codec, chunk.getCodec());
                assertEquals(c.uncompressedSize(), chunk.getUncompressedSize());
                assertEquals(Digests.crc32(chunk.getPayload().toByteArray()), chunk.getCrc32());
                assertEquals(chunk.getCrc32(), c.crc32(codec));
                assertTrue(c.verifyCrc(codec));
            }
        }
        assertEquals(-1, s.verifyAll());
    }

    @Test public void hashesRecompute() throws Exception {
        Snapshot s = encode(TestStores.sized(120), Set.of());
        List<byte[]> entityHashes = new ArrayList<>();
        int[] types = new int[s.chunkCount()], counts = new int[s.chunkCount()];
        for (EntityKind k : EnumSet.allOf(EntityKind.class)) {
            MessageDigest d = Digests.sha256();
            EntityManifest m = s.manifest(k);
            for (int i = 0; i < m.getChunkCount(); i++) {
                SnapshotChunk chunk = s.chunk(m.getFirstSeq() + i).decode(Codec.CODEC_NONE);
                d.update(chunk.getPayload().toByteArray());
                types[chunk.getSeq()] = chunk.getEntityType();
                counts[chunk.getSeq()] = chunk.getRecordCount();
            }
            byte[] h = d.digest();
            assertArrayEquals(h, s.entityHash(k));
            assertArrayEquals(h, m.getEntitySha256().toByteArray());
            entityHashes.add(h);
        }
        byte[] content = Digests.contentSha256(entityHashes);
        assertArrayEquals(content, s.contentSha256());
        assertArrayEquals(Digests.snapshotId(Framing.SCHEMA_MAJOR, Framing.SCHEMA_MINOR, types, counts, content), s.snapshotId());
        assertEquals(16, s.snapshotId().length);
    }

    @Test public void deterministic() {
        Snapshot a = encode(TestStores.sized(80), ALL);
        Snapshot b = encode(TestStores.sized(80), ALL);
        assertArrayEquals(a.snapshotId(), b.snapshotId());
        assertEquals(a.chunkCount(), b.chunkCount());
        for (int i = 0; i < a.chunkCount(); i++) {
            for (Codec codec : a.codecs()) assertArrayEquals(a.chunk(i).frame(codec), b.chunk(i).frame(codec));
        }
        Snapshot c = encode(TestStores.sized(81), ALL);
        assertFalse(Arrays.equals(a.snapshotId(), c.snapshotId()));
    }

    @Test public void compressedVariantsDecompressToTheSamePayload() throws Exception {
        Snapshot s = encode(TestStores.sized(100), ALL);
        assertEquals(Set.of(Codec.CODEC_NONE, Codec.CODEC_LZ4, Codec.CODEC_ZSTD), s.codecs());
        for (EncodedChunk c : s.chunks()) {
            byte[] plain = c.decode(Codec.CODEC_NONE).getPayload().toByteArray();
            for (Codec codec : List.of(Codec.CODEC_LZ4, Codec.CODEC_ZSTD)) {
                SnapshotChunk chunk = c.decode(codec);
                assertEquals(plain.length, chunk.getUncompressedSize());
                assertArrayEquals(plain, PayloadCodecs.decompress(codec, chunk.getPayload().toByteArray(), chunk.getUncompressedSize()));
            }
        }
        assertTrue(s.totalFrameBytes(Codec.CODEC_ZSTD) < s.totalFrameBytes(Codec.CODEC_NONE));
    }

    @Test public void frameBuffersAreSharedReadOnlyAndUnreleasable() {
        Snapshot s = encode(TestStores.small(), Set.of());
        EncodedChunk c = s.chunk(0);
        ByteBuf one = c.frameBuffer(Codec.CODEC_NONE);
        ByteBuf two = c.frameBuffer(Codec.CODEC_NONE);
        assertTrue(one.isReadOnly());
        assertEquals(c.frameBytes(Codec.CODEC_NONE), one.readableBytes());
        one.readInt();
        assertEquals(c.frameBytes(Codec.CODEC_NONE), two.readableBytes());   // independent indexes
        assertFalse(one.release());
        assertEquals(c.frameBytes(Codec.CODEC_NONE), c.frameBuffer(Codec.CODEC_NONE).readableBytes());
        byte[] copy = new byte[two.readableBytes()];
        two.getBytes(0, copy);
        assertArrayEquals(c.frame(Codec.CODEC_NONE), copy);
    }

    @Test public void emptyKindsGetZeroChunksButAManifestEntry() {
        RefDataStore store = TestStores.build(1, 0, 0, 0).store();
        Snapshot s = encode(store, Set.of());
        assertEquals(3, s.chunkCount());   // country, currency, client
        assertEquals(0, s.manifest(EntityKind.ADDRESS).getChunkCount());
        assertEquals(3, s.manifest(EntityKind.ADDRESS).getFirstSeq());
        assertArrayEquals(Digests.sha256().digest(), s.entityHash(EntityKind.SETTLEMENT_INSTRUCTION));
        assertNotEquals(s.snapshotIdHex(), encode(TestStores.small(), Set.of()).snapshotIdHex());
    }

    @Test(expected = IllegalArgumentException.class) public void targetTooCloseToFrameLimitIsRejected() {
        new SnapshotEncoder(FRAME_MAX - Framing.CHUNK_HEADROOM_BYTES + 1, FRAME_MAX, Set.of());
    }

    // ------------------------------------------------------------------ helpers

    interface BatchDecoder<T> { List<T> decode(byte[] payload) throws Exception; }

    private static <T> List<T> decodeAll(Snapshot s, EntityKind kind, BatchDecoder<T> decoder) throws Exception {
        List<T> out = new ArrayList<>();
        ByteArrayOutputStream all = new ByteArrayOutputStream();
        EntityManifest m = s.manifest(kind);
        for (int i = 0; i < m.getChunkCount(); i++) {
            SnapshotChunk chunk = s.chunk(m.getFirstSeq() + i).decode(Codec.CODEC_NONE);
            List<T> items = decoder.decode(chunk.getPayload().toByteArray());
            assertEquals(chunk.getRecordCount(), items.size());
            out.addAll(items);
            all.write(chunk.getPayload().toByteArray());
        }
        // concatenated payloads are one big batch
        assertEquals(out, decoder.decode(all.toByteArray()));
        return out;
    }

    /** Size of the first length-delimited item (tag + varint length + bytes) in a batch payload. */
    private static int firstItemSize(byte[] payload) {
        int i = 1, len = 0, shift = 0;
        while (true) {
            int b = payload[i++] & 0xff;
            len |= (b & 0x7f) << shift;
            if ((b & 0x80) == 0) break;
            shift += 7;
        }
        return i + len;
    }
}
