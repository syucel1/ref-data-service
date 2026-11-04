package com.example.refdata.model;

import com.example.refdata.model.index.AsciiIndex;
import com.example.refdata.model.index.CsrIndex;
import com.example.refdata.model.index.LongIndex;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * The immutable, indexed in-memory reference data. Built once by {@link RefDataStoreBuilder}; every method is
 * read-only, lock-free and allocation-free (returns an existing object, a primitive, or null when absent).
 * Rows of each kind are held in primary-key order; "position" means the index into that order.
 */
public final class RefDataStore {
    final Country[] countries;
    final Currency[] currencies;
    final Client[] clients;
    final Address[] addresses;
    final Account[] accounts;
    final SettlementInstruction[] ssis;

    final AsciiIndex countryByCode, countryByIso3, currencyByCode, clientByRef, clientByLei, accountByNumber;
    final int[] countryByNumeric, currencyByNumeric;           // 1000 slots each, -1 = none
    final LongIndex clientById, addressById, accountById, ssiById;
    final CsrIndex childrenOfClient, addressesOfClient, accountsOfClient, ssisOfAccount;
    private final long dataVersion;

    RefDataStore(Country[] countries, Currency[] currencies, Client[] clients, Address[] addresses, Account[] accounts,
                 SettlementInstruction[] ssis, AsciiIndex countryByCode, AsciiIndex countryByIso3, int[] countryByNumeric,
                 AsciiIndex currencyByCode, int[] currencyByNumeric, LongIndex clientById, AsciiIndex clientByRef,
                 AsciiIndex clientByLei, LongIndex addressById, LongIndex accountById, AsciiIndex accountByNumber,
                 LongIndex ssiById, CsrIndex childrenOfClient, CsrIndex addressesOfClient, CsrIndex accountsOfClient,
                 CsrIndex ssisOfAccount, long dataVersion) {
        this.countries = countries; this.currencies = currencies; this.clients = clients; this.addresses = addresses;
        this.accounts = accounts; this.ssis = ssis; this.countryByCode = countryByCode; this.countryByIso3 = countryByIso3;
        this.countryByNumeric = countryByNumeric; this.currencyByCode = currencyByCode; this.currencyByNumeric = currencyByNumeric;
        this.clientById = clientById; this.clientByRef = clientByRef; this.clientByLei = clientByLei; this.addressById = addressById;
        this.accountById = accountById; this.accountByNumber = accountByNumber; this.ssiById = ssiById;
        this.childrenOfClient = childrenOfClient; this.addressesOfClient = addressesOfClient; this.accountsOfClient = accountsOfClient;
        this.ssisOfAccount = ssisOfAccount; this.dataVersion = dataVersion;
    }

    /** Release id of the data this store was built from (0 when the source has none). */
    public long dataVersion() { return dataVersion; }

    // ---- counts and iteration (read-only views over the primary-key order) ----
    public int count(EntityKind kind) {
        return switch (kind) {
            case COUNTRY -> countries.length; case CURRENCY -> currencies.length; case CLIENT -> clients.length;
            case ADDRESS -> addresses.length; case ACCOUNT -> accounts.length; case SETTLEMENT_INSTRUCTION -> ssis.length;
        };
    }
    public long totalRows() { long t = 0; for (EntityKind k : EntityKind.values()) t += count(k); return t; }
    public List<Country> countries() { return Collections.unmodifiableList(Arrays.asList(countries)); }
    public List<Currency> currencies() { return Collections.unmodifiableList(Arrays.asList(currencies)); }
    public List<Client> clients() { return Collections.unmodifiableList(Arrays.asList(clients)); }
    public List<Address> addresses() { return Collections.unmodifiableList(Arrays.asList(addresses)); }
    public List<Account> accounts() { return Collections.unmodifiableList(Arrays.asList(accounts)); }
    public List<SettlementInstruction> settlementInstructions() { return Collections.unmodifiableList(Arrays.asList(ssis)); }

    /** The rows of any kind, in primary-key order (read-only view; use the typed methods when the kind is known). */
    public List<?> rows(EntityKind kind) {
        return switch (kind) {
            case COUNTRY -> countries(); case CURRENCY -> currencies(); case CLIENT -> clients();
            case ADDRESS -> addresses(); case ACCOUNT -> accounts(); case SETTLEMENT_INSTRUCTION -> settlementInstructions();
        };
    }

    /**
     * Looks a row up by its textual primary key ({@link EntityKind#keyOf}): ISO code for countries and currencies,
     * decimal id otherwise. Returns null when absent.
     * @throws NumberFormatException when a numeric key is not a number
     */
    public Object findByKey(EntityKind kind, String key) {
        return switch (kind) {
            case COUNTRY -> country(key); case CURRENCY -> currency(key);
            case CLIENT -> client(Long.parseLong(key)); case ADDRESS -> address(Long.parseLong(key));
            case ACCOUNT -> account(Long.parseLong(key)); case SETTLEMENT_INSTRUCTION -> settlementInstruction(Long.parseLong(key));
        };
    }

    // ---- countries and currencies ----
    public Country country(CharSequence code) { int p = countryByCode.get(code); return p < 0 ? null : countries[p]; }
    public Country countryByIso3(CharSequence iso3) { int p = countryByIso3.get(iso3); return p < 0 ? null : countries[p]; }
    public Country countryByNumeric(int numeric) { int p = numeric >= 0 && numeric < 1000 ? countryByNumeric[numeric] : -1; return p < 0 ? null : countries[p]; }
    public Currency currency(CharSequence code) { int p = currencyByCode.get(code); return p < 0 ? null : currencies[p]; }
    public Currency currencyByNumeric(int numeric) { int p = numeric >= 0 && numeric < 1000 ? currencyByNumeric[numeric] : -1; return p < 0 ? null : currencies[p]; }

    // ---- clients ----
    public Client client(long id) { int p = clientById.get(id); return p < 0 ? null : clients[p]; }
    public Client clientByRef(CharSequence ref) { int p = clientByRef.get(ref); return p < 0 ? null : clients[p]; }
    public Client clientByLei(CharSequence lei) { int p = clientByLei.get(lei); return p < 0 ? null : clients[p]; }
    /** The parent client, or null. */
    public Client parentOf(Client c) { return c.hasParent() ? client(c.parentClientId()) : null; }
    /** The top of the hierarchy the client belongs to (the client itself if it has no parent). */
    public Client ultimateParentOf(Client c) {
        Client cur = c;
        for (int guard = 0; guard < 1024 && cur.hasParent(); guard++) { Client p = client(cur.parentClientId()); if (p == null) break; cur = p; }
        return cur;
    }
    public int childCount(long clientId) { return childrenOfClient.count(clientById.get(clientId)); }
    /** The i-th child (0 <= i < childCount), or null for an unknown client. */
    public Client child(long clientId, int i) { int p = clientById.get(clientId); return p < 0 ? null : clients[childrenOfClient.child(p, i)]; }

    // ---- addresses ----
    public Address address(long id) { int p = addressById.get(id); return p < 0 ? null : addresses[p]; }
    public int addressCountOfClient(long clientId) { return addressesOfClient.count(clientById.get(clientId)); }
    /** Addresses of a client, ordered by type then validFrom. */
    public Address addressOfClient(long clientId, int i) { int p = clientById.get(clientId); return p < 0 ? null : addresses[addressesOfClient.child(p, i)]; }
    /** The primary address of the given type valid on the given epoch day, or null. */
    public Address primaryAddress(long clientId, AddressType type, int epochDay) {
        int p = clientById.get(clientId);
        int n = addressesOfClient.count(p);
        for (int i = 0; i < n; i++) {
            Address a = addresses[addressesOfClient.child(p, i)];
            if (a.type() == type && a.primary() && a.validOn(epochDay)) return a;
        }
        return null;
    }

    // ---- accounts ----
    public Account account(long id) { int p = accountById.get(id); return p < 0 ? null : accounts[p]; }
    public Account accountByNumber(CharSequence number) { int p = accountByNumber.get(number); return p < 0 ? null : accounts[p]; }
    public int accountCountOfClient(long clientId) { return accountsOfClient.count(clientById.get(clientId)); }
    public Account accountOfClient(long clientId, int i) { int p = clientById.get(clientId); return p < 0 ? null : accounts[accountsOfClient.child(p, i)]; }

    // ---- settlement instructions ----
    public SettlementInstruction settlementInstruction(long id) { int p = ssiById.get(id); return p < 0 ? null : ssis[p]; }
    public int ssiCountOfAccount(long accountId) { return ssisOfAccount.count(accountById.get(accountId)); }
    /** Instructions of an account, ordered by currency, asset class, effectiveFrom. */
    public SettlementInstruction ssiOfAccount(long accountId, int i) { int p = accountById.get(accountId); return p < 0 ? null : ssis[ssisOfAccount.child(p, i)]; }
    /** The default instruction for the account, currency and asset class effective on the epoch day, or null. */
    public SettlementInstruction defaultSsi(long accountId, CharSequence currencyCode, AssetClass assetClass, int epochDay) {
        int p = accountById.get(accountId);
        int n = ssisOfAccount.count(p);
        for (int i = 0; i < n; i++) {
            SettlementInstruction s = ssis[ssisOfAccount.child(p, i)];
            if (s.isDefault() && s.assetClass() == assetClass && s.effectiveOn(epochDay) && s.currencyCode().contentEquals(currencyCode)) return s;
        }
        return null;
    }
}
