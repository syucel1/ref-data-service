package com.example.refdata.server;

import com.example.refdata.model.EntityKind;
import com.example.refdata.model.RefDataSink;
import com.example.refdata.model.RefDataSource;
import com.example.refdata.model.RefDataStore;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StoreLoaderTest {
    /** A source that replays a hand-made store, failing a configurable number of times first. */
    private static class FakeSource implements RefDataSource {
        final RefDataStore store = TestStores.small();
        int preflightFailures, loadFailures;
        long versionAfterLoad = 42;
        final List<String> phases = new ArrayList<>();

        @Override public Preflight preflight() throws Exception {
            if (preflightFailures-- > 0) throw new IllegalStateException("db down");
            Map<EntityKind, Long> counts = new EnumMap<>(EntityKind.class);
            for (EntityKind k : EntityKind.values()) counts.put(k, (long) store.count(k));
            return new Preflight(counts, 42, "fake");
        }

        @Override public long load(RefDataSink sink) throws Exception {
            if (loadFailures-- > 0) throw new IllegalStateException("query failed");
            store.countries().forEach(sink::country);
            store.currencies().forEach(sink::currency);
            store.clients().forEach(sink::client);
            store.addresses().forEach(sink::address);
            store.accounts().forEach(sink::account);
            store.settlementInstructions().forEach(sink::settlementInstruction);
            return versionAfterLoad;
        }
    }

    private static ServerConfig config(String... kv) {
        Properties p = new Properties();
        p.setProperty("rds.db.connect.retries", "2");
        p.setProperty("rds.load.retries", "1");
        for (int i = 0; i < kv.length; i += 2) p.setProperty(kv[i], kv[i + 1]);
        return ServerConfig.fromProperties(p);
    }

    private static StoreLoader loader(ServerConfig c, FakeSource src) {
        return new StoreLoader(c, Duration.ofMillis(1), src.phases::add);
    }

    @Test public void loadsBuildsAndReportsPhases() {
        FakeSource src = new FakeSource();
        StoreLoader.Loaded l = loader(config(), src).load(src);
        assertEquals(src.store.totalRows(), l.store().totalRows());
        assertEquals(42, l.store().dataVersion());
        assertEquals(1, l.loadAttempts());
        assertTrue(l.validation().isClean());
        assertEquals(List.of("connecting", "loading", "validating"), src.phases);
        assertEquals(3L, (long) l.loadedRows().get(EntityKind.CLIENT));
        assertTrue(l.heapEstimateBytes() > 0);
        assertEquals(l.heapAssumptions(), loader(config(), src).heapAssumptions());
    }

    @Test public void retriesTheConnectionThenGivesUp() {
        FakeSource src = new FakeSource();
        src.preflightFailures = 2;
        assertEquals(1, loader(config(), src).load(src).loadAttempts());
        src = new FakeSource();
        src.preflightFailures = 3;
        try { loader(config(), src).load(src); fail(); }
        catch (StoreLoader.StartupException e) { assertTrue(e.getMessage(), e.getMessage().contains("unreachable after 3 attempt(s)")); }
    }

    @Test public void retriesTheFullLoad() {
        FakeSource src = new FakeSource();
        src.loadFailures = 1;
        assertEquals(2, loader(config(), src).load(src).loadAttempts());
        src = new FakeSource();
        src.loadFailures = 2;
        try { loader(config(), src).load(src); fail(); }
        catch (StoreLoader.StartupException e) { assertTrue(e.getMessage(), e.getMessage().contains("after 2 attempt(s)")); }
    }

    @Test public void dataVersionChangeDuringLoadIsRetried() {
        FakeSource src = new FakeSource();
        src.versionAfterLoad = 43;
        try { loader(config(), src).load(src); fail(); }
        catch (StoreLoader.StartupException e) { assertTrue(e.getMessage(), e.getMessage().contains("data version changed")); }
    }

    @Test public void sanityMinimumFailsFast() {
        FakeSource src = new FakeSource();
        try { loader(config("rds.table.min.rows.client", "1000"), src).load(src); fail(); }
        catch (StoreLoader.StartupException e) { assertTrue(e.getMessage(), e.getMessage().contains("CLIENT has 3 rows, below the sanity minimum 1000")); }
        assertEquals(List.of("connecting"), src.phases);
    }

    @Test public void heapEstimateFailsWhenTooSmall() {
        StoreLoader l = loader(config("rds.codecs", "LZ4,ZSTD"), new FakeSource());
        Map<EntityKind, Long> large = new EnumMap<>(EntityKind.class);
        for (EntityKind k : EntityKind.values()) large.put(k, 1_000_000L);
        long estimate = l.estimateHeap(large);
        assertTrue(estimate > 1_000_000L * (320 + 480 + 220 + 240) * StoreLoader.PEAK_FACTOR);
        assertTrue(l.heapAssumptions(), l.heapAssumptions().contains("address=480") && l.heapAssumptions().contains("x 3 codec(s)") && l.heapAssumptions().contains("peak factor 1.8"));
        l.checkHeap(large, estimate + 1);
        try { l.checkHeap(large, estimate - 1); fail(); }
        catch (StoreLoader.StartupException e) { assertTrue(e.getMessage(), e.getMessage().contains("heap is too small")); }
    }

    @Test public void validationFailureNamesTheRule() {
        FakeSource src = new FakeSource() {
            @Override public long load(RefDataSink sink) throws Exception {
                long v = super.load(sink);
                sink.client(new com.example.refdata.model.Client(99, "REF-99", null, "Orphan", null, com.example.refdata.model.ClientType.FUND,
                        com.example.refdata.model.Status.ACTIVE, "ZZ", 0, 1, 1, 1, 1));
                return v;
            }
        };
        try { loader(config(), src).load(src); fail(); }
        catch (StoreLoader.StartupException e) { assertTrue(e.getMessage(), e.getMessage().contains("MISSING_REFERENCE")); }
    }
}
