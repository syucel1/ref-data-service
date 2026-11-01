package com.example.refdata.server;

import com.example.refdata.contract.Digests;
import com.example.refdata.db.DataGenerator;
import com.example.refdata.db.EmbeddedDerby;
import com.example.refdata.loader.jdbc.JdbcLoadConfig;
import com.example.refdata.loader.jdbc.JdbcRefDataSource;
import com.example.refdata.model.EntityKind;
import com.example.refdata.protocol.v1.Envelope;
import com.example.refdata.protocol.v1.Hello;
import com.example.refdata.protocol.v1.SnapshotChunk;
import com.example.refdata.protocol.v1.SnapshotRequest;
import com.example.refdata.testing.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.security.MessageDigest;
import java.sql.Connection;
import java.util.EnumMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** The whole service against a generated in-memory Derby database, read back with a raw client. */
@Category(IntegrationTest.class)
public class EndToEndDerbyIT {
    @Test public void loadsFromDerbyAndServesAVerifiableSnapshot() throws Exception {
        try (EmbeddedDerby db = EmbeddedDerby.inMemory("rds-e2e-" + System.nanoTime())) {
            DataGenerator.Result generated;
            try (Connection c = db.connect()) { generated = new DataGenerator(500, 7).generate(c); }
            Properties p = new Properties();
            p.setProperty("rds.tcp.port", "0");
            p.setProperty("rds.http.port", "0");
            p.setProperty("rds.tcp.bind.address", "127.0.0.1");
            p.setProperty("rds.db.url", db.url());
            p.setProperty("rds.codecs", "ZSTD");
            p.setProperty("rds.chunk.target.bytes", "64k");
            p.setProperty("rds.frame.max.bytes", "128k");
            ServerConfig config = ServerConfig.fromProperties(p);
            RefDataServer server = RefDataServer.start(config, new JdbcRefDataSource(db.url(), new JdbcLoadConfig(config.loadFetchSize, config.loadParallelism, true)));
            try {
                for (EntityKind k : EntityKind.values()) assertEquals(k.name(), (long) generated.rowCounts().get(k), server.snapshot().manifest(k).getRecordCount());
                assertEquals(DataGenerator.RELEASE_ID, server.snapshot().dataVersion());
                assertTrue(server.startupReport().validation().isClean());
                try (Socket s = new Socket("127.0.0.1", server.tcpPort())) {
                    s.setSoTimeout(10_000);
                    DataOutputStream out = new DataOutputStream(s.getOutputStream());
                    DataInputStream in = new DataInputStream(s.getInputStream());
                    write(out, Envelope.newBuilder().setHello(Hello.newBuilder().setProtocolMajor(1).addSchemaMajors(1).setClientName("e2e")).build());
                    assertEquals(Envelope.BodyCase.HELLO_ACK, read(in).getBodyCase());
                    write(out, Envelope.newBuilder().setSnapshotRequest(SnapshotRequest.newBuilder().setRequestId(1)).build());
                    assertEquals(Envelope.BodyCase.SNAPSHOT_BEGIN, read(in).getBodyCase());
                    Map<EntityKind, MessageDigest> digests = new EnumMap<>(EntityKind.class);
                    Map<EntityKind, Long> records = new EnumMap<>(EntityKind.class);
                    int expectedSeq = 0;
                    Envelope e;
                    while ((e = read(in)).getBodyCase() == Envelope.BodyCase.SNAPSHOT_CHUNK) {
                        SnapshotChunk ch = e.getSnapshotChunk();
                        assertEquals(expectedSeq++, ch.getSeq());
                        byte[] payload = ch.getPayload().toByteArray();
                        assertEquals(ch.getCrc32(), Digests.crc32(payload));
                        EntityKind k = EntityKind.fromWire(ch.getEntityType());
                        digests.computeIfAbsent(k, x -> Digests.sha256()).update(com.example.refdata.server.snapshot.PayloadCodecs.decompress(ch.getCodec(), payload, ch.getUncompressedSize()));
                        records.merge(k, (long) ch.getRecordCount(), Long::sum);
                    }
                    assertEquals(Envelope.BodyCase.SNAPSHOT_END, e.getBodyCase());
                    assertEquals(server.snapshot().chunkCount(), e.getSnapshotEnd().getChunksSent());
                    for (EntityKind k : EntityKind.values()) {
                        assertArrayEquals(k.name(), server.snapshot().entityHash(k), digests.get(k).digest());
                        assertEquals(k.name(), (long) generated.rowCounts().get(k), (long) records.get(k));
                    }
                }
            } finally {
                server.close();
            }
        }
    }

    private static void write(DataOutputStream out, Envelope e) throws Exception {
        byte[] b = e.toByteArray();
        out.writeInt(b.length);
        out.write(b);
        out.flush();
    }

    private static Envelope read(DataInputStream in) throws Exception {
        byte[] b = new byte[in.readInt()];
        in.readFully(b);
        return Envelope.parseFrom(b);
    }
}
