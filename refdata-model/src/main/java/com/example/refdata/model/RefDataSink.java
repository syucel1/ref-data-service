package com.example.refdata.model;

/**
 * Receives entities as a {@link RefDataSource} streams them; implemented by {@link RefDataStoreBuilder}.
 * <p>Threading contract: a source may call the methods for <em>different</em> kinds from different threads at the
 * same time (one thread per table), but must never call the <em>same</em> method from two threads, and must make
 * the loading threads' work visible to the thread that later builds the store (e.g. by joining them).
 */
public interface RefDataSink {
    void country(Country c);
    void currency(Currency c);
    void client(Client c);
    void address(Address a);
    void account(Account a);
    void settlementInstruction(SettlementInstruction s);
}
