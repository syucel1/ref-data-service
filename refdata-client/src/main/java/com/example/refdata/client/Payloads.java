package com.example.refdata.client;

import com.example.refdata.protocol.v1.Codec;
import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdException;
import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

/** Restores the uncompressed batch bytes of a chunk payload (codecs as fixed in the implementation notes). */
final class Payloads {
    private static final LZ4FastDecompressor LZ4 = LZ4Factory.fastestInstance().fastDecompressor();
    /** Far above any chunk the server can send (chunk target + headroom must fit the 1 MiB frame); guards the allocation. */
    static final int MAX_UNCOMPRESSED_BYTES = 64 << 20;

    private Payloads() {}

    /**
     * @throws IllegalArgumentException when the payload does not decompress to exactly {@code uncompressedSize} bytes
     *                                  or the codec is unknown
     */
    static byte[] decompress(Codec codec, byte[] payload, int uncompressedSize) {
        if (uncompressedSize < 0 || uncompressedSize > MAX_UNCOMPRESSED_BYTES) {
            throw new IllegalArgumentException("uncompressed_size " + Integer.toUnsignedString(uncompressedSize) + " is out of range");
        }
        switch (codec) {
            case CODEC_UNSPECIFIED, CODEC_NONE -> {
                if (uncompressedSize != 0 && uncompressedSize != payload.length) {
                    throw new IllegalArgumentException("uncompressed_size " + uncompressedSize + " but payload has " + payload.length + " bytes");
                }
                return payload;
            }
            case CODEC_LZ4 -> {
                byte[] out = new byte[uncompressedSize];
                int consumed;
                try {
                    consumed = LZ4.decompress(payload, 0, out, 0, uncompressedSize);
                } catch (LZ4Exception e) {
                    throw new IllegalArgumentException("LZ4 payload is corrupt", e);
                }
                if (consumed != payload.length) throw new IllegalArgumentException("LZ4 payload has trailing bytes");
                return out;
            }
            case CODEC_ZSTD -> {
                byte[] out = new byte[uncompressedSize];
                long produced;
                try {
                    produced = Zstd.decompressByteArray(out, 0, uncompressedSize, payload, 0, payload.length);
                } catch (ZstdException e) {
                    throw new IllegalArgumentException("ZSTD payload is corrupt", e);
                }
                if (produced != uncompressedSize) throw new IllegalArgumentException("ZSTD payload decompressed to " + produced + " bytes, expected " + uncompressedSize);
                return out;
            }
            default -> throw new IllegalArgumentException("unknown codec " + codec);
        }
    }
}
