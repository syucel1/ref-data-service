package com.example.refdata.server.net;

import com.example.refdata.contract.Framing;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import java.nio.ByteOrder;

/**
 * Splits the inbound byte stream into frames (4-byte big-endian length, excluded from the frame passed on).
 * A zero length is a {@link CorruptedFrameException} (PROTOCOL_VIOLATION); a length over
 * {@link Framing#SERVER_INBOUND_MAX_FRAME_BYTES} fails fast with a {@code TooLongFrameException} (FRAME_TOO_LARGE).
 */
final class FrameDecoder extends LengthFieldBasedFrameDecoder {
    FrameDecoder() {
        super(Framing.SERVER_INBOUND_MAX_FRAME_BYTES, 0, Framing.LENGTH_PREFIX_BYTES, 0, Framing.LENGTH_PREFIX_BYTES, true);
    }

    @Override protected long getUnadjustedFrameLength(ByteBuf buf, int offset, int length, ByteOrder order) {
        long len = buf.getUnsignedInt(offset);
        if (len == 0) throw new CorruptedFrameException("zero-length frame");
        return len;
    }
}
