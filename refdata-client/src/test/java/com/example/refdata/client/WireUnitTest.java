package com.example.refdata.client;

import com.example.refdata.contract.Framing;
import com.example.refdata.protocol.v1.Codec;
import com.example.refdata.protocol.v1.Envelope;
import com.example.refdata.protocol.v1.Heartbeat;
import com.github.luben.zstd.Zstd;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** The small building blocks: framing, back-off, endpoint parsing, payload codecs. */
public class WireUnitTest {
    @Test public void framesRoundTripWithTheFourBytePrefix() throws IOException {
        Envelope hb = Envelope.newBuilder().setHeartbeat(Heartbeat.newBuilder().setSentAtEpochMs(42)).build();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        new FrameIO(new ByteArrayInputStream(new byte[0]), out, 1024).write(hb);
        byte[] bytes = out.toByteArray();
        int body = hb.getSerializedSize();
        assertEquals(Framing.LENGTH_PREFIX_BYTES + body, bytes.length);
        assertEquals(body, ((bytes[0] & 0xff) << 24) | ((bytes[1] & 0xff) << 16) | ((bytes[2] & 0xff) << 8) | (bytes[3] & 0xff));
        FrameIO in = new FrameIO(new ByteArrayInputStream(bytes), new ByteArrayOutputStream(), 1024);
        assertEquals(hb, in.read());
        assertEquals(bytes.length, in.bytesRead());
    }

    @Test public void zeroAndOversizedFrameLengthsAreProtocolErrors() {
        byte[] zero = {0, 0, 0, 0};
        assertThrows(ProtocolException.class, () -> new FrameIO(new ByteArrayInputStream(zero), new ByteArrayOutputStream(), 1024).read());
        byte[] huge = {'G', 'E', 'T', ' '};
        assertThrows(ProtocolException.class, () -> new FrameIO(new ByteArrayInputStream(huge), new ByteArrayOutputStream(), 1 << 20).read());
    }

    @Test public void backoffGrowsWithJitterAndIsCapped() {
        Backoff b = new Backoff(Duration.ofMillis(100), Duration.ofMillis(1000), new Random(1));
        Duration d1 = b.next(), d2 = b.next(), d3 = b.next(), d4 = b.next(), d5 = b.next();
        assertTrue(d1.toMillis() >= 50 && d1.toMillis() <= 100);
        assertTrue(d2.toMillis() >= 100 && d2.toMillis() <= 200);
        assertTrue(d3.toMillis() >= 200 && d3.toMillis() <= 400);
        assertTrue(d4.toMillis() >= 400 && d4.toMillis() <= 800);
        assertTrue(d5.toMillis() >= 500 && d5.toMillis() <= 1000);
        b.reset();
        assertTrue(b.next().toMillis() <= 100);
    }

    @Test public void endpointParsing() {
        assertEquals(new Endpoint("rds-a", 7400), Endpoint.parse("rds-a:7400"));
        assertThrows(IllegalArgumentException.class, () -> Endpoint.parse("rds-a"));
        assertThrows(IllegalArgumentException.class, () -> Endpoint.parse("rds-a:x"));
        assertThrows(IllegalArgumentException.class, () -> Endpoint.parse("rds-a:70000"));
    }

    @Test public void payloadsDecompressAndRejectWrongSizes() {
        byte[] data = "reference data reference data reference data".getBytes();
        byte[] zstd = Zstd.compress(data);
        assertArrayEquals(data, Payloads.decompress(Codec.CODEC_ZSTD, zstd, data.length));
        assertThrows(IllegalArgumentException.class, () -> Payloads.decompress(Codec.CODEC_ZSTD, zstd, data.length + 1));
        assertArrayEquals(data, Payloads.decompress(Codec.CODEC_NONE, data, data.length));
        assertThrows(IllegalArgumentException.class, () -> Payloads.decompress(Codec.CODEC_NONE, data, data.length - 1));
        assertThrows(IllegalArgumentException.class, () -> Payloads.decompress(Codec.CODEC_LZ4, new byte[] {1, 2, 3}, 100));
    }

    @Test public void builderRejectsMissingSettings() {
        assertThrows(IllegalStateException.class, () -> RefDataClient.builder().clientName("x").build());
        assertThrows(IllegalStateException.class, () -> RefDataClient.builder().endpoints("h:1").build());
        assertThrows(IllegalArgumentException.class, () -> RefDataClient.builder().maxFrameBytes(1024));
    }
}
