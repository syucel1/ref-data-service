package com.example.refdata.client;

import com.example.refdata.contract.Framing;
import com.example.refdata.protocol.v1.Envelope;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Reads and writes frames: a 4-byte big-endian length (excluding itself) followed by exactly that many bytes of one
 * {@link Envelope} (design section 9.2). Not thread-safe; one reader and one writer at a time is fine.
 */
final class FrameIO {
    private final DataInputStream in;
    private final DataOutputStream out;
    private final int maxFrameBytes;
    private long bytesRead;

    FrameIO(InputStream in, OutputStream out, int maxFrameBytes) {
        this.in = new DataInputStream(new BufferedInputStream(in, 64 << 10));
        this.out = new DataOutputStream(new BufferedOutputStream(out, 8 << 10));
        this.maxFrameBytes = maxFrameBytes;
    }

    /**
     * Reads the next frame.
     * @throws EOFException      when the peer closed the connection
     * @throws ProtocolException when the length is 0, above the limit, or the body is not an Envelope
     */
    Envelope read() throws IOException {
        int length = in.readInt();
        if (length <= 0 || length > maxFrameBytes) {
            throw new ProtocolException("bad frame length " + Integer.toUnsignedString(length) + " (limit " + maxFrameBytes + ")");
        }
        byte[] body = new byte[length];
        in.readFully(body);
        bytesRead += Framing.LENGTH_PREFIX_BYTES + length;
        try {
            return Envelope.parseFrom(body);
        } catch (InvalidProtocolBufferException e) {
            throw new ProtocolException("frame body is not an Envelope", e);
        }
    }

    void write(Envelope envelope) throws IOException {
        byte[] body = envelope.toByteArray();
        out.writeInt(body.length);
        out.write(body);
        out.flush();
    }

    /** Bytes consumed so far, prefixes included. */
    long bytesRead() { return bytesRead; }
}
