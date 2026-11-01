package com.example.refdata.client;

import com.example.refdata.contract.Digests;
import com.example.refdata.model.Account;
import com.example.refdata.model.Client;
import com.example.refdata.model.EntityKind;
import com.example.refdata.model.RefDataStore;
import com.example.refdata.protocol.v1.Codec;

import java.time.Duration;
import java.util.SplittableRandom;

/**
 * Command-line demo: loads a snapshot from {@code host:port}, prints counts and a few lookups, then times one
 * million random primary-key lookups.
 * <p>Usage: {@code RefDataClientDemo host:port [host:port ...] [--lz4|--zstd]}
 */
public final class RefDataClientDemo {
    private static final int LOOKUPS = 1_000_000;

    private RefDataClientDemo() {}

    public static void main(String[] args) {
        RefDataClient.Builder builder = RefDataClient.builder().clientName("refdata-client-demo").startupTimeout(Duration.ofMinutes(5));
        int endpoints = 0;
        for (String arg : args) {
            switch (arg) {
                case "--lz4" -> builder.acceptedCodecs(Codec.CODEC_LZ4);
                case "--zstd" -> builder.acceptedCodecs(Codec.CODEC_ZSTD);
                default -> { builder.endpoints(arg); endpoints++; }
            }
        }
        if (endpoints == 0) {
            System.err.println("usage: RefDataClientDemo host:port [host:port ...] [--lz4|--zstd]");
            System.exit(2);
        }
        LoadedSnapshot loaded = builder.build().load();
        RefDataStore store = loaded.store();
        System.out.printf("snapshot %s, data version %d, schema %d.%d%n", Digests.hex(loaded.info().getSnapshotId().toByteArray()),
                loaded.info().getDataVersion(), loaded.info().getSchemaMajor(), loaded.info().getSchemaMinor());
        for (EntityKind k : EntityKind.values()) System.out.printf("  %-22s %,d%n", k, store.count(k));
        LoadStats s = loaded.stats();
        System.out.printf("load: %,d bytes, %,d chunks, %d connections, %d resumes, %d restarts, %s total, by phase %s%n",
                s.bytesReceived(), s.chunksAccepted(), s.connections(), s.resumes(), s.fullRestarts(), s.totalDuration(), s.durationByPhase());

        if (!store.clients().isEmpty()) {
            Client c = store.clients().get(store.clients().size() / 2);
            System.out.printf("client %d: ref=%s name=%s country=%s accounts=%d addresses=%d%n", c.id(), c.ref(), c.legalName(),
                    c.incorporationCountry(), store.accountCountOfClient(c.id()), store.addressCountOfClient(c.id()));
            System.out.printf("  byRef(%s) -> %s%n", c.ref(), store.clientByRef(c.ref()) == c);
        }
        if (!store.accounts().isEmpty()) {
            Account a = store.accounts().get(0);
            System.out.printf("account %d: number=%s currency=%s ssis=%d byNumber -> %s%n", a.id(), a.number(), a.baseCurrency(),
                    store.ssiCountOfAccount(a.id()), store.accountByNumber(a.number()) == a);
        }
        System.out.printf("country GB -> %s, currency USD -> %s%n", store.country("GB"), store.currency("USD"));

        measureLookups(store);
    }

    private static void measureLookups(RefDataStore store) {
        int clients = store.clients().size();
        int accounts = store.accounts().size();
        if (clients == 0 || accounts == 0) {
            System.out.println("no clients/accounts to look up");
            return;
        }
        long[] clientIds = new long[clients];
        for (int i = 0; i < clients; i++) clientIds[i] = store.clients().get(i).id();
        long[] accountIds = new long[accounts];
        for (int i = 0; i < accounts; i++) accountIds[i] = store.accounts().get(i).id();
        SplittableRandom rnd = new SplittableRandom(42);
        int hits = 0;
        for (int round = 0; round < 2; round++) {          // first round warms up, second is measured
            hits = 0;
            long start = System.nanoTime();
            for (int i = 0; i < LOOKUPS; i++) {
                if ((i & 1) == 0) {
                    if (store.client(clientIds[rnd.nextInt(clients)]) != null) hits++;
                } else {
                    if (store.account(accountIds[rnd.nextInt(accounts)]) != null) hits++;
                }
            }
            long nanos = System.nanoTime() - start;
            if (round == 1) System.out.printf("%,d random lookups (client/account by id): %.1f ms total, %.1f ns per lookup, %,d hits%n",
                    LOOKUPS, nanos / 1e6, (double) nanos / LOOKUPS, hits);
        }
    }
}
