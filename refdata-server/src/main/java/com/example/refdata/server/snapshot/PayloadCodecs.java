package com.example.refdata.server.snapshot;

import com.example.refdata.protocol.v1.Codec;
import com.github.luben.zstd.Zstd;
import net.jpountz.lz4.LZ4Factory;

/**
 * Compression of chunk payloads as fixed by the implementation notes: LZ4 = lz4-java block format with the fastest
 * instance, ZSTD = zstd-jni at its default level. Decompression needs the uncompressed size, which every chunk carries.
 */
public final class PayloadCodecs {
    private static final LZ4Factory LZ4 = LZ4Factory.fastestInstance();

    private PayloadCodecs() {}

    /** Returns the payload as it travels with the given codec; NONE and UNSPECIFIED return the input itself. */
    public static byte[] compress(Codec codec, byte[] uncompressed) {
        return switch (codec) {
            case CODEC_LZ4 -> LZ4.fastCompressor().compress(uncompressed);
            case CODEC_ZSTD -> Zstd.compress(uncompressed);
            case CODEC_NONE, CODEC_UNSPECIFIED -> uncompressed;
            case UNRECOGNIZED -> throw new IllegalArgumentException("unknown codec");
        };
    }

    /** The inverse of {@link #compress}. */
    public static byte[] decompress(Codec codec, byte[] payload, int uncompressedSize) {
        return switch (codec) {
            case CODEC_LZ4 -> LZ4.fastDecompressor().decompress(payload, uncompressedSize);
            case CODEC_ZSTD -> Zstd.decompress(payload, uncompressedSize);
            case CODEC_NONE, CODEC_UNSPECIFIED -> payload;
            case UNRECOGNIZED -> throw new IllegalArgumentException("unknown codec");
        };
    }
}
