package com.example.refdata.server.net;

import com.example.refdata.contract.Framing;
import com.example.refdata.protocol.v1.Envelope;
import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.util.List;

/** Envelope &lt;-&gt; bytes. The encoder writes the length prefix too; pre-encoded chunk frames (ByteBuf) pass through untouched. */
final class EnvelopeCodec {
    private EnvelopeCodec() {}

    /** Inbound: one de-framed ByteBuf -&gt; Envelope; bytes that do not parse are a {@link CorruptedFrameException}. */
    static final class Decoder extends MessageToMessageDecoder<ByteBuf> {
        @Override protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
            try {
                out.add(Envelope.parseFrom(ByteBufUtil.getBytes(in)));
            } catch (InvalidProtocolBufferException e) {
                throw new CorruptedFrameException("frame is not an Envelope: " + e.getMessage());
            }
        }
    }

    /** Outbound: Envelope -&gt; length prefix + serialized bytes. Stateless, so shared by every channel. */
    @ChannelHandler.Sharable
    static final class Encoder extends MessageToByteEncoder<Envelope> {
        @Override protected void encode(ChannelHandlerContext ctx, Envelope msg, ByteBuf out) {
            int size = msg.getSerializedSize();
            out.ensureWritable(Framing.LENGTH_PREFIX_BYTES + size);
            out.writeInt(size);
            out.writeBytes(msg.toByteArray());
        }
    }
}
