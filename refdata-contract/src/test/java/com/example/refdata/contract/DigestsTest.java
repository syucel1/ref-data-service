package com.example.refdata.contract;

import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.Assert.*;

public class DigestsTest {

    @Test public void crc32IsZlibCrc32() {
        // well-known check value of CRC-32/ISO-HDLC for "123456789"
        assertEquals(0xCBF43926, Digests.crc32("123456789".getBytes(StandardCharsets.US_ASCII)));
        byte[] buf = "xx123456789yy".getBytes(StandardCharsets.US_ASCII);
        assertEquals(0xCBF43926, Digests.crc32(buf, 2, 9));
    }

    @Test public void contentHashIsOverEntityHashesInOrder() {
        byte[] a = Digests.sha256().digest("a".getBytes()), b = Digests.sha256().digest("b".getBytes());
        byte[] ab = Digests.contentSha256(List.of(a, b));
        byte[] ba = Digests.contentSha256(List.of(b, a));
        assertEquals(32, ab.length);
        assertFalse(java.util.Arrays.equals(ab, ba));
        assertArrayEquals(ab, Digests.contentSha256(List.of(a, b)));
    }

    @Test public void snapshotIdDependsOnLayoutAndContent() {
        byte[] content = Digests.sha256().digest("c".getBytes());
        byte[] id1 = Digests.snapshotId(1, 0, new int[]{1, 3}, new int[]{10, 20}, content);
        byte[] id2 = Digests.snapshotId(1, 0, new int[]{1, 3}, new int[]{10, 21}, content);
        byte[] id3 = Digests.snapshotId(1, 1, new int[]{1, 3}, new int[]{10, 20}, content);
        assertEquals(16, id1.length);
        assertArrayEquals(id1, Digests.snapshotId(1, 0, new int[]{1, 3}, new int[]{10, 20}, content));
        assertFalse(java.util.Arrays.equals(id1, id2));
        assertFalse(java.util.Arrays.equals(id1, id3));
        assertEquals(32, Digests.hex(id1).length());
    }

    @Test public void frozenProtocolConstants() {
        // Field numbers the design freezes for all protocol majors (protocol.proto header)
        assertEquals(1, com.example.refdata.protocol.v1.Envelope.HELLO_FIELD_NUMBER);
        assertEquals(2, com.example.refdata.protocol.v1.Envelope.HELLO_ACK_FIELD_NUMBER);
        assertEquals(8, com.example.refdata.protocol.v1.Envelope.ERROR_FIELD_NUMBER);
        assertEquals(1, com.example.refdata.protocol.v1.Hello.PROTOCOL_MAJOR_FIELD_NUMBER);
        assertEquals(4, com.example.refdata.protocol.v1.Hello.MAX_FRAME_BYTES_FIELD_NUMBER);
        assertEquals(2, com.example.refdata.protocol.v1.ErrorReply.CODE_FIELD_NUMBER);
        assertEquals(2, com.example.refdata.protocol.v1.ErrorCode.ERROR_CODE_UNSUPPORTED_PROTOCOL_VERSION_VALUE);
        assertEquals(7, com.example.refdata.protocol.v1.ErrorCode.ERROR_CODE_SERVER_BUSY_VALUE);
        assertEquals(1, Framing.PROTOCOL_MAJOR);
        assertTrue(Framing.CHUNK_HEADROOM_BYTES < Framing.DEFAULT_MAX_FRAME_BYTES);
    }
}
