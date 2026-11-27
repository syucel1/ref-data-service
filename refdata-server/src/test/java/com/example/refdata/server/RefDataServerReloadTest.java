package com.example.refdata.server;

import com.example.refdata.model.Address;
import com.example.refdata.model.Country;
import com.example.refdata.model.EntityKind;
import com.example.refdata.model.RefDataSink;
import com.example.refdata.model.RefDataSource;
import com.example.refdata.model.RefDataStore;
import com.example.refdata.model.validation.ValidationException;
import com.example.refdata.protocol.v1.Envelope;
import com.example.refdata.protocol.v1.ErrorCode;
import com.example.refdata.protocol.v1.Hello;
import com.example.refdata.protocol.v1.HelloAck;
import com.example.refdata.protocol.v1.SnapshotRequest;
import com.example.refdata.server.admin.ReloadRejectedException;
import com.example.refdata.server.admin.ReloadResult;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.EnumMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Milestone 11: explicit full reload, without a database (an in-memory {@link RefDataSource}). */
public class RefDataServerReloadTest {

    /** Serves whatever store the test sets, optionally with one extra (invalid) address, or fails. */
    private static final class StoreSource implements RefDataSource {
        volatile RefDataStore store;
        volatile Address extraAddress;
        volatile boolean unreachable;

        StoreSource(RefDataStore store) { this.store = store; }

        @Override public Preflight preflight() throws IOException {
            if (unreachable) throw new IOException("database unreachable");
            Map<EntityKind, Long> counts = new EnumMap<>(EntityKind.class);
            for (EntityKind k : EntityKind.values()) counts.put(k, (long) store.count(k));
            if (extraAddress != null) counts.merge(EntityKind.ADDRESS, 1L, Long::sum);
            return new Preflight(counts, store.dataVersion(), "in-memory test source");
        }

        @Override public long load(RefDataSink sink) {
            for (EntityKind k : EntityKind.values()) for (Object row : store.rows(k)) sink.accept(k, row);
            if (extraAddress != null) sink.address(extraAddress);
            return store.dataVersion();
        }
    }

    private static ServerConfig config() {
        Properties p = new Properties();
        p.setProperty("rds.tcp.port", "0");
        p.setProperty("rds.http.port", "0");
        p.setProperty("rds.io.threads", "1");
        p.setProperty("rds.db.connect.retries", "0");
        p.setProperty("rds.load.retries", "0");
        p.setProperty("rds.heartbeat.interval", "1s");
        return ServerConfig.fromProperties(p);
    }

    /** {@link TestStores#small(long)} with country GB renamed: what a direct SQL fix might change. */
    private static RefDataStore renamedGb(long version, String name) {
        RefDataStore base = TestStores.small(version);
        com.example.refdata.model.RefDataStoreBuilder b = new com.example.refdata.model.RefDataStoreBuilder().dataVersion(version);
        for (EntityKind k : EntityKind.values()) {
            for (Object row : base.rows(k)) {
                if (row instanceof Country c && c.code().equals("GB")) row = new Country(c.code(), c.iso3(), c.numericCode(), name, c.region());
                b.accept(k, row);
            }
        }
        return b.build().store();
    }

    @Test public void reloadPublishesTheDatabaseAsANewSnapshotGeneration() throws Exception {
        StoreSource source = new StoreSource(TestStores.small(42));
        try (RefDataServer server = RefDataServer.start(config(), source)) {
            String before = server.snapshot().snapshotIdHex();
            try (Socket live = new Socket("127.0.0.1", server.tcpPort())) {
                live.setSoTimeout(10_000);
                DataOutputStream out = new DataOutputStream(live.getOutputStream());
                DataInputStream in = new DataInputStream(live.getInputStream());
                HelloAck ack = liveHello(out, in);
                write(out, Envelope.newBuilder().setSnapshotRequest(SnapshotRequest.newBuilder().setRequestId(1)).build());
                Envelope e;
                while ((e = read(in)).getBodyCase() != Envelope.BodyCase.SNAPSHOT_END) { /* chunks */ }

                source.store = renamedGb(43, "Fixed by SQL");
                ReloadResult result = server.reload();

                assertEquals(42, result.previousDataVersion());
                assertEquals(43, result.dataVersion());
                assertEquals(server.snapshot().snapshotIdHex(), result.snapshotId());
                assertNotEquals(before, result.snapshotId());
                assertEquals(3L, (long) result.rows().get(EntityKind.COUNTRY));
                assertEquals("Fixed by SQL", server.currentStore().country("GB").name());
                assertEquals(1, server.metrics().reloads("published"));
                assertTrue(server.liveHistory().isEmpty());

                Envelope resync = readSkippingHeartbeats(in);          // the live session is told to reload
                assertEquals(ErrorCode.ERROR_CODE_SNAPSHOT_NOT_AVAILABLE, resync.getError().getCode());
                assertTrue(resync.getError().getFatal());

                try (Socket next = new Socket("127.0.0.1", server.tcpPort())) {   // new connections get the new data and generation
                    next.setSoTimeout(10_000);
                    HelloAck nextAck = liveHello(new DataOutputStream(next.getOutputStream()), new DataInputStream(next.getInputStream()));
                    assertNotEquals(ack.getUpdateGeneration(), nextAck.getUpdateGeneration());
                    assertEquals(server.snapshot().info(), nextAck.getCurrentSnapshot());
                }
            }
        }
    }

    @Test public void reloadAtTheSameDataVersionStillPublishesTheFixedContent() throws Exception {
        StoreSource source = new StoreSource(TestStores.small(42));
        try (RefDataServer server = RefDataServer.start(config(), source)) {
            source.store = renamedGb(42, "Fixed without a release row");
            ReloadResult result = server.reload();
            assertEquals(42, result.dataVersion());
            assertEquals("Fixed without a release row", server.currentStore().country("GB").name());
        }
    }

    @Test public void databaseBehindTheServedVersionIsRefusedAndNothingChanges() throws Exception {
        StoreSource source = new StoreSource(TestStores.small(42));
        try (RefDataServer server = RefDataServer.start(config(), source)) {
            RefDataStore served = server.currentStore();
            source.store = renamedGb(41, "Older data");
            try { server.reload(); fail("expected a refusal"); }
            catch (ReloadRejectedException expected) { assertTrue(expected.getMessage(), expected.getMessage().contains("behind")); }
            assertSame(served, server.currentStore());
            assertEquals(1, server.metrics().reloads("rejected"));
        }
    }

    @Test public void invalidDatabaseContentIsRejectedAndThePreviousDataStaysServed() throws Exception {
        StoreSource source = new StoreSource(TestStores.small(42));
        try (RefDataServer server = RefDataServer.start(config(), source)) {
            String snapshotId = server.snapshot().snapshotIdHex();
            Address any = server.currentStore().addresses().get(0);
            source.store = TestStores.small(43);
            source.extraAddress = new Address(999, 12345, any.type(), "Orphan", null, null, "Nowhere", null, "PC", "GB", true,
                    any.validFrom(), Address.OPEN_ENDED, 1, any.updatedAt());
            try { server.reload(); fail("expected a validation failure"); }
            catch (ValidationException expected) { assertTrue(expected.report().toString(), !expected.report().isClean()); }
            assertEquals(snapshotId, server.snapshot().snapshotIdHex());
            assertEquals(42, server.currentStore().dataVersion());
        }
    }

    @Test public void unreachableDatabaseFailsTheReloadWithoutPublishing() throws Exception {
        StoreSource source = new StoreSource(TestStores.small(42));
        try (RefDataServer server = RefDataServer.start(config(), source)) {
            String snapshotId = server.snapshot().snapshotIdHex();
            source.unreachable = true;
            try { server.reload(); fail("expected an I/O failure"); }
            catch (IOException expected) { assertTrue(expected.getMessage(), expected.getMessage().contains("unreachable")); }
            assertEquals(snapshotId, server.snapshot().snapshotIdHex());
            assertEquals(1, server.metrics().reloads("failed"));
        }
    }

    @Test public void serverStartedFromABuiltStoreCannotReload() throws Exception {
        try (RefDataServer server = RefDataServer.startWithStore(config(), TestStores.small(), "unit")) {
            try { server.reload(); fail("expected unsupported"); }
            catch (UnsupportedOperationException expected) { assertTrue(true); }
        }
    }

    private static HelloAck liveHello(DataOutputStream out, DataInputStream in) throws IOException {
        write(out, Envelope.newBuilder().setHello(Hello.newBuilder().setProtocolMajor(1).addSchemaMajors(1).setClientName("reload-test")
                .setKeepAlive(true).setLiveUpdates(true).setMaxFrameBytes(16 << 20)).build());
        Envelope ack = readSkippingHeartbeats(in);
        assertEquals(Envelope.BodyCase.HELLO_ACK, ack.getBodyCase());
        assertTrue(ack.getHelloAck().getLiveUpdates());
        return ack.getHelloAck();
    }

    private static void write(DataOutputStream out, Envelope e) throws IOException {
        byte[] bytes = e.toByteArray();
        out.writeInt(bytes.length);
        out.write(bytes);
        out.flush();
    }

    private static Envelope read(DataInputStream in) throws IOException {
        int size = in.readInt();
        if (size < 1 || size > (16 << 20)) throw new IOException("bad frame length " + size);
        return Envelope.parseFrom(in.readNBytes(size));
    }

    private static Envelope readSkippingHeartbeats(DataInputStream in) throws IOException {
        Envelope e;
        do { e = read(in); } while (e.hasHeartbeat());
        return e;
    }
}
