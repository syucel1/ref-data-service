package com.example.refdata.contract;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.zip.CRC32;

/** The integrity arithmetic both sides must agree on (design sections 9.7 and 9.8). */
public final class Digests {
    private Digests() {}

    public static MessageDigest sha256() {
        try { return MessageDigest.getInstance("SHA-256"); }
        catch (NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
    }

    /** zlib CRC-32 of the bytes exactly as they appear in the frame. */
    public static int crc32(byte[] data, int off, int len) {
        CRC32 c = new CRC32();
        c.update(data, off, len);
        return (int) c.getValue();
    }

    public static int crc32(byte[] data) { return crc32(data, 0, data.length); }

    /** Whole-snapshot hash: SHA-256 over the per-entity hashes in manifest order. */
    public static byte[] contentSha256(List<byte[]> entityShas) {
        MessageDigest d = sha256();
        for (byte[] e : entityShas) d.update(e);
        return d.digest();
    }

    /**
     * Content-derived snapshot id: first 16 bytes of SHA-256 over (schema major, schema minor, the chunk layout as the
     * sequence of (entity type, record count) pairs, the whole-snapshot hash). Same data + same build + same chunk
     * settings => same id on every instance.
     */
    public static byte[] snapshotId(int schemaMajor, int schemaMinor, int[] chunkEntityTypes, int[] chunkRecordCounts, byte[] contentSha256) {
        MessageDigest d = sha256();
        d.update("refdata-snapshot-id".getBytes(StandardCharsets.US_ASCII));
        ByteBuffer b = ByteBuffer.allocate(8 + chunkEntityTypes.length * 8);
        b.putInt(schemaMajor).putInt(schemaMinor);
        for (int i = 0; i < chunkEntityTypes.length; i++) b.putInt(chunkEntityTypes[i]).putInt(chunkRecordCounts[i]);
        d.update(b.array(), 0, b.position());
        d.update(contentSha256);
        byte[] full = d.digest();
        byte[] id = new byte[16];
        System.arraycopy(full, 0, id, 0, 16);
        return id;
    }

    public static String hex(byte[] b) {
        StringBuilder sb = new StringBuilder(b.length * 2);
        for (byte x : b) sb.append(Character.forDigit((x >> 4) & 0xf, 16)).append(Character.forDigit(x & 0xf, 16));
        return sb.toString();
    }
}
