package com.example.refdata.server;

import com.example.refdata.protocol.v1.Envelope;
import com.example.refdata.protocol.v1.Hello;
import com.example.refdata.protocol.v1.SnapshotRequest;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.HttpURLConnection;
import java.net.Socket;
import java.net.URI;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class RefDataServerTest {
    @Test public void optionalInspectionLifecycle() throws Exception {
        try (RefDataServer disabled = RefDataServer.startWithStore(ephemeral(), TestStores.small(), "unit")) {
            assertEquals(-1, disabled.inspectionPort());
        }
        Properties p = new Properties();
        p.setProperty("rds.tcp.port", "0");
        p.setProperty("rds.http.port", "0");
        p.setProperty("rds.inspection.enabled", "true");
        p.setProperty("rds.inspection.port", "0");
        p.setProperty("rds.io.threads", "1");
        int port;
        try (RefDataServer enabled = RefDataServer.startWithStore(ServerConfig.fromProperties(p), TestStores.small(), "unit")) {
            port = enabled.inspectionPort();
            assertEquals(200, http(port, "/?cache=CLIENT&limit=1"));
        }
        try { http(port, "/"); throw new AssertionError("inspection port should be closed"); }
        catch (java.io.IOException expected) { /* closed */ }
    }
    private static ServerConfig ephemeral() {
        Properties p = new Properties();
        p.setProperty("rds.tcp.port", "0");
        p.setProperty("rds.http.port", "0");
        p.setProperty("rds.tcp.bind.address", "127.0.0.1");
        p.setProperty("rds.io.threads", "1");
        p.setProperty("rds.shutdown.drain.timeout", "1s");
        return ServerConfig.fromProperties(p);
    }

    private static int http(int port, String path) throws Exception {
        HttpURLConnection c = (HttpURLConnection) URI.create("http://127.0.0.1:" + port + path).toURL().openConnection();
        c.setConnectTimeout(2000); c.setReadTimeout(2000);
        return c.getResponseCode();
    }

    @Test public void startsWithStoreOnEphemeralPortsAndServesASnapshot() throws Exception {
        RefDataServer server = RefDataServer.startWithStore(ephemeral(), TestStores.sized(20), "unit");
        int httpPort = server.httpPort();
        try {
            assertNotEquals(0, server.tcpPort());
            assertNotEquals(0, server.httpPort());
            assertEquals(200, http(server.httpPort(), "/health/ready"));
            assertEquals(200, http(server.httpPort(), "/startup-report"));
            assertEquals(server.snapshot().snapshotIdHex(), server.startupReport().snapshot().snapshotIdHex());
            try (Socket s = new Socket("127.0.0.1", server.tcpPort())) {
                s.setSoTimeout(5000);
                DataOutputStream out = new DataOutputStream(s.getOutputStream());
                DataInputStream in = new DataInputStream(s.getInputStream());
                write(out, Envelope.newBuilder().setHello(Hello.newBuilder().setProtocolMajor(1).addSchemaMajors(1).setClientName("lifecycle")).build());
                assertEquals(Envelope.BodyCase.HELLO_ACK, read(in).getBodyCase());
                write(out, Envelope.newBuilder().setSnapshotRequest(SnapshotRequest.newBuilder().setRequestId(1)).build());
                assertEquals(Envelope.BodyCase.SNAPSHOT_BEGIN, read(in).getBodyCase());
                int chunks = 0;
                Envelope e;
                while ((e = read(in)).getBodyCase() == Envelope.BodyCase.SNAPSHOT_CHUNK) chunks++;
                assertEquals(Envelope.BodyCase.SNAPSHOT_END, e.getBodyCase());
                assertEquals(server.snapshot().chunkCount(), chunks);
            }
        } finally {
            server.close();
        }
        server.awaitTermination();
        try { http(httpPort, "/health/live"); throw new AssertionError("HTTP port should be closed"); }
        catch (java.io.IOException expected) { assertTrue(true); }
    }

    @Test public void portInUseFailsCleanly() throws Exception {
        RefDataServer first = RefDataServer.startWithStore(ephemeral(), TestStores.small(), "unit");
        try {
            Properties p = new Properties();
            p.setProperty("rds.tcp.port", String.valueOf(first.tcpPort()));
            p.setProperty("rds.http.port", "0");
            p.setProperty("rds.tcp.bind.address", "127.0.0.1");
            try { RefDataServer.startWithStore(ServerConfig.fromProperties(p), TestStores.small(), "unit"); throw new AssertionError("expected failure"); }
            catch (IllegalStateException e) { assertTrue(e.getMessage(), e.getMessage().contains("cannot bind TCP port " + first.tcpPort())); }
        } finally {
            first.close();
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
