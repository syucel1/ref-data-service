package com.example.refdata.model;

import com.example.refdata.model.index.AsciiIndex;
import com.example.refdata.model.index.CsrIndex;
import com.example.refdata.model.index.LongIndex;
import com.example.refdata.model.validation.ValidationException;
import com.example.refdata.model.validation.ValidationPolicy;
import com.example.refdata.model.validation.ValidationReport;
import com.example.refdata.model.validation.Violation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.ToLongFunction;

/**
 * Collects rows (as a {@link RefDataSink}), then validates, indexes and freezes them into a {@link RefDataStore}.
 * While collecting, each kind has its own list and no state is shared between kinds, so the {@link RefDataSink}
 * threading contract holds (different kinds from different threads, never the same kind concurrently). The resulting
 * store is immutable.
 */
public final class RefDataStoreBuilder implements RefDataSink {
    private final List<Country> countries = new ArrayList<>();
    private final List<Currency> currencies = new ArrayList<>();
    private final List<Client> clients = new ArrayList<>();
    private final List<Address> addresses = new ArrayList<>();
    private final List<Account> accounts = new ArrayList<>();
    private final List<SettlementInstruction> ssis = new ArrayList<>();

    private ValidationPolicy policy = ValidationPolicy.FAIL;
    private double denseThreshold = 1.5;
    private final Map<EntityKind, Long> minRows = new EnumMap<>(EntityKind.class);
    private long dataVersion;
    private Set<EntityKind> loadedKinds = java.util.EnumSet.allOf(EntityKind.class);

    /**
     * Declares which kinds were loaded (a consumer may request a subset). References into a kind that was not loaded
     * are not checked, so an account whose client was not requested is fine (design row C-13).
     */
    public RefDataStoreBuilder loadedKinds(Set<EntityKind> kinds) { this.loadedKinds = java.util.EnumSet.copyOf(kinds); return this; }

    public RefDataStoreBuilder policy(ValidationPolicy p) { this.policy = p; return this; }
    public RefDataStoreBuilder denseThreshold(double t) { this.denseThreshold = t; return this; }
    /** Fails the build when a kind has fewer rows than this (guards against a truncated database). */
    public RefDataStoreBuilder minRows(EntityKind kind, long rows) { minRows.put(kind, rows); return this; }
    public RefDataStoreBuilder dataVersion(long v) { this.dataVersion = v; return this; }
    /** Lets the builder size its lists up front. */
    public RefDataStoreBuilder expect(EntityKind kind, long rows) {
        int n = (int) Math.min(rows, Integer.MAX_VALUE - 16);
        switch (kind) {
            case COUNTRY -> ((ArrayList<Country>) countries).ensureCapacity(n);
            case CURRENCY -> ((ArrayList<Currency>) currencies).ensureCapacity(n);
            case CLIENT -> ((ArrayList<Client>) clients).ensureCapacity(n);
            case ADDRESS -> ((ArrayList<Address>) addresses).ensureCapacity(n);
            case ACCOUNT -> ((ArrayList<Account>) accounts).ensureCapacity(n);
            case SETTLEMENT_INSTRUCTION -> ((ArrayList<SettlementInstruction>) ssis).ensureCapacity(n);
        }
        return this;
    }

    @Override public void country(Country c) { countries.add(c); }
    @Override public void currency(Currency c) { currencies.add(c); }
    @Override public void client(Client c) { clients.add(c); }
    @Override public void address(Address a) { addresses.add(a); }
    @Override public void account(Account a) { accounts.add(a); }
    @Override public void settlementInstruction(SettlementInstruction s) { ssis.add(s); }

    public int collected(EntityKind kind) {
        return switch (kind) {
            case COUNTRY -> countries.size(); case CURRENCY -> currencies.size(); case CLIENT -> clients.size();
            case ADDRESS -> addresses.size(); case ACCOUNT -> accounts.size(); case SETTLEMENT_INSTRUCTION -> ssis.size();
        };
    }

    /** Result of a build: the store and what validation found. */
    public record Result(RefDataStore store, ValidationReport report) {
    }

    /**
     * Validates, indexes and freezes. With policy FAIL any violation throws {@link ValidationException}; with QUARANTINE
     * the offending rows and their dependants are dropped (table-level violations such as too few rows always fail).
     */
    public Result build() {
        ValidationReport report = new ValidationReport();
        Country[] co = countries.toArray(new Country[0]);
        Currency[] cu = currencies.toArray(new Currency[0]);
        Client[] cl = clients.toArray(new Client[0]);
        Address[] ad = addresses.toArray(new Address[0]);
        Account[] ac = accounts.toArray(new Account[0]);
        SettlementInstruction[] ss = ssis.toArray(new SettlementInstruction[0]);
        Arrays.sort(co, Comparator.comparing(Country::code));
        Arrays.sort(cu, Comparator.comparing(Currency::code));
        Arrays.sort(cl, Comparator.comparingLong(Client::id));
        Arrays.sort(ad, Comparator.comparingLong(Address::id));
        Arrays.sort(ac, Comparator.comparingLong(Account::id));
        Arrays.sort(ss, Comparator.comparingLong(SettlementInstruction::id));

        Validator v = new Validator(co, cu, cl, ad, ac, ss, report, minRows, loadedKinds);
        v.run();
        if (!report.isClean()) {
            if (policy == ValidationPolicy.FAIL || v.hasTableLevelViolation()) throw new ValidationException(report);
            // quarantine: drop bad rows and everything that depends on them, then re-validate
            co = v.keep(co, EntityKind.COUNTRY, Country[]::new);
            cu = v.keep(cu, EntityKind.CURRENCY, Currency[]::new);
            cl = v.keep(cl, EntityKind.CLIENT, Client[]::new);
            ad = v.keep(ad, EntityKind.ADDRESS, Address[]::new);
            ac = v.keep(ac, EntityKind.ACCOUNT, Account[]::new);
            ss = v.keep(ss, EntityKind.SETTLEMENT_INSTRUCTION, SettlementInstruction[]::new);
            ValidationReport second = new ValidationReport();
            Validator v2 = new Validator(co, cu, cl, ad, ac, ss, second, minRows, loadedKinds);
            v2.run();
            if (!second.isClean()) throw new ValidationException(second);   // cascading removal should leave it clean
        }
        return new Result(index(co, cu, cl, ad, ac, ss), report);
    }

    private RefDataStore index(Country[] co, Currency[] cu, Client[] cl, Address[] ad, Account[] ac, SettlementInstruction[] ss) {
        AsciiIndex countryByCode = AsciiIndex.build(Arrays.stream(co).map(Country::code).toArray(String[]::new), co.length);
        AsciiIndex countryByIso3 = AsciiIndex.build(Arrays.stream(co).map(Country::iso3).toArray(String[]::new), co.length);
        int[] countryByNumeric = numeric(co.length, i -> co[i].numericCode());
        AsciiIndex currencyByCode = AsciiIndex.build(Arrays.stream(cu).map(Currency::code).toArray(String[]::new), cu.length);
        int[] currencyByNumeric = numeric(cu.length, i -> cu[i].numericCode());

        LongIndex clientById = LongIndex.build(ids(cl, Client::id), cl.length, denseThreshold);
        AsciiIndex clientByRef = AsciiIndex.build(Arrays.stream(cl).map(Client::ref).toArray(String[]::new), cl.length);
        AsciiIndex clientByLei = AsciiIndex.build(Arrays.stream(cl).map(Client::lei).toArray(String[]::new), cl.length);
        LongIndex addressById = LongIndex.build(ids(ad, Address::id), ad.length, denseThreshold);
        LongIndex accountById = LongIndex.build(ids(ac, Account::id), ac.length, denseThreshold);
        AsciiIndex accountByNumber = AsciiIndex.build(Arrays.stream(ac).map(Account::number).toArray(String[]::new), ac.length);
        LongIndex ssiById = LongIndex.build(ids(ss, SettlementInstruction::id), ss.length, denseThreshold);

        int[] parentOfClient = new int[cl.length];
        for (int i = 0; i < cl.length; i++) parentOfClient[i] = cl[i].hasParent() ? clientById.get(cl[i].parentClientId()) : -1;
        CsrIndex children = CsrIndex.build(parentOfClient, cl.length, cl.length, null);

        int[] clientOfAddress = new int[ad.length];
        for (int i = 0; i < ad.length; i++) clientOfAddress[i] = clientById.get(ad[i].clientId());
        Integer[] addrOrder = order(ad.length);
        Arrays.sort(addrOrder, Comparator.<Integer>comparingInt(i -> ad[i].type().ordinal()).thenComparingInt(i -> ad[i].validFrom()).thenComparingLong(i -> ad[i].id()));
        CsrIndex addressesOfClient = CsrIndex.build(clientOfAddress, ad.length, cl.length, unbox(addrOrder));

        int[] clientOfAccount = new int[ac.length];
        for (int i = 0; i < ac.length; i++) clientOfAccount[i] = clientById.get(ac[i].clientId());
        CsrIndex accountsOfClient = CsrIndex.build(clientOfAccount, ac.length, cl.length, null);

        int[] accountOfSsi = new int[ss.length];
        for (int i = 0; i < ss.length; i++) accountOfSsi[i] = accountById.get(ss[i].accountId());
        Integer[] ssiOrder = order(ss.length);
        Arrays.sort(ssiOrder, Comparator.<Integer, String>comparing(i -> ss[i].currencyCode()).thenComparingInt(i -> ss[i].assetClass().ordinal()).thenComparingInt(i -> ss[i].effectiveFrom()).thenComparingLong(i -> ss[i].id()));
        CsrIndex ssisOfAccount = CsrIndex.build(accountOfSsi, ss.length, ac.length, unbox(ssiOrder));

        return new RefDataStore(co, cu, cl, ad, ac, ss, countryByCode, countryByIso3, countryByNumeric, currencyByCode, currencyByNumeric,
                clientById, clientByRef, clientByLei, addressById, accountById, accountByNumber, ssiById,
                children, addressesOfClient, accountsOfClient, ssisOfAccount, dataVersion);
    }

    private static <T> long[] ids(T[] rows, ToLongFunction<T> id) {
        long[] out = new long[rows.length];
        for (int i = 0; i < rows.length; i++) out[i] = id.applyAsLong(rows[i]);
        return out;
    }

    private static int[] numeric(int n, java.util.function.IntUnaryOperator codeAt) {
        int[] slots = new int[1000];
        Arrays.fill(slots, -1);
        for (int i = 0; i < n; i++) { int c = codeAt.applyAsInt(i); if (c >= 0 && c < 1000) slots[c] = i; }
        return slots;
    }

    private static Integer[] order(int n) { Integer[] o = new Integer[n]; for (int i = 0; i < n; i++) o[i] = i; return o; }
    private static int[] unbox(Integer[] o) { int[] r = new int[o.length]; for (int i = 0; i < o.length; i++) r[i] = o[i]; return r; }

    // =====================================================================================================
    /**
     * All validation rules (design section 13.1, rows S-5 to S-9 and S-14). Works on the sorted arrays with
     * primitive lookups (binary search over the sorted ids, sorted copies for unique strings, packed long keys for
     * effective-dated groups) so that it needs no per-row boxed objects at the large profile.
     */
    static final class Validator {
        private final Country[] co; private final Currency[] cu; private final Client[] cl;
        private final Address[] ad; private final Account[] ac; private final SettlementInstruction[] ss;
        private final ValidationReport report;
        private final Map<EntityKind, Long> minRows;
        private final Set<EntityKind> loaded;
        private final boolean[] badCo, badCu, badCl, badAd, badAc, badSs;
        private final long[] clientIds, accountIds;
        private boolean tableLevel;

        Validator(Country[] co, Currency[] cu, Client[] cl, Address[] ad, Account[] ac, SettlementInstruction[] ss,
                  ValidationReport report, Map<EntityKind, Long> minRows, Set<EntityKind> loaded) {
            this.co = co; this.cu = cu; this.cl = cl; this.ad = ad; this.ac = ac; this.ss = ss; this.report = report; this.minRows = minRows; this.loaded = loaded;
            badCo = new boolean[co.length]; badCu = new boolean[cu.length]; badCl = new boolean[cl.length];
            badAd = new boolean[ad.length]; badAc = new boolean[ac.length]; badSs = new boolean[ss.length];
            clientIds = ids(cl, Client::id); accountIds = ids(ac, Account::id);
        }

        boolean hasTableLevelViolation() { return tableLevel; }

        void run() {
            minRows();
            for (int i = 0; i < co.length; i++) {
                Country c = co[i];
                if (blank(c.code()) || blank(c.iso3()) || blank(c.name())) flag(EntityKind.COUNTRY, i, c.code(), Violation.Rule.MANDATORY_MISSING, "code, iso3 and name are mandatory");
                else if (!upperAlpha(c.code(), 2) || !upperAlpha(c.iso3(), 3)) flag(EntityKind.COUNTRY, i, c.code(), Violation.Rule.INVALID_VALUE, "codes must be A-Z, 2 and 3 letters");
                if (i > 0 && co[i - 1].code().equals(c.code())) flag(EntityKind.COUNTRY, i, c.code(), Violation.Rule.DUPLICATE_KEY, "duplicate country_code " + c.code());
            }
            uniqueStrings(EntityKind.COUNTRY, co.length, i -> co[i].iso3(), i -> co[i].code(), "iso3");
            uniqueInts(EntityKind.COUNTRY, co.length, i -> co[i].numericCode(), i -> co[i].code(), "numeric_code");
            for (int i = 0; i < cu.length; i++) {
                Currency c = cu[i];
                if (blank(c.code()) || blank(c.name())) flag(EntityKind.CURRENCY, i, c.code(), Violation.Rule.MANDATORY_MISSING, "code and name are mandatory");
                else if (!upperAlpha(c.code(), 3)) flag(EntityKind.CURRENCY, i, c.code(), Violation.Rule.INVALID_VALUE, "code must be 3 letters A-Z");
                if (c.minorUnits() < 0 || c.minorUnits() > 6) flag(EntityKind.CURRENCY, i, c.code(), Violation.Rule.INVALID_VALUE, "minor_units out of range: " + c.minorUnits());
                if (i > 0 && cu[i - 1].code().equals(c.code())) flag(EntityKind.CURRENCY, i, c.code(), Violation.Rule.DUPLICATE_KEY, "duplicate currency_code " + c.code());
            }
            uniqueInts(EntityKind.CURRENCY, cu.length, i -> cu[i].numericCode(), i -> cu[i].code(), "numeric_code");
            Map<String, Integer> countryPos = codePositions(co.length, i -> co[i].code(), badCo);
            Map<String, Integer> currencyPos = codePositions(cu.length, i -> cu[i].code(), badCu);

            for (int i = 0; i < cl.length; i++) {
                Client c = cl[i];
                if (i > 0 && cl[i - 1].id() == c.id()) flag(EntityKind.CLIENT, i, Violation.Rule.DUPLICATE_KEY, "duplicate client_id");
                if (blank(c.ref()) || blank(c.legalName()) || c.type() == null || c.status() == null || blank(c.incorporationCountry()))
                    flag(EntityKind.CLIENT, i, Violation.Rule.MANDATORY_MISSING, "client_ref, legal_name, client_type, status, incorporation_country are mandatory");
                if (c.lei() != null && c.lei().length() != 20) flag(EntityKind.CLIENT, i, Violation.Rule.INVALID_VALUE, "lei must be 20 characters");
                if (c.riskRating() < 0 || c.riskRating() > 10) flag(EntityKind.CLIENT, i, Violation.Rule.INVALID_VALUE, "risk_rating out of range: " + c.riskRating());
                if (has(EntityKind.COUNTRY) && c.incorporationCountry() != null && !countryPos.containsKey(c.incorporationCountry())) flag(EntityKind.CLIENT, i, Violation.Rule.MISSING_REFERENCE, "incorporation_country " + c.incorporationCountry() + " not in COUNTRY");
                if (c.hasParent()) {
                    if (c.parentClientId() == c.id()) flag(EntityKind.CLIENT, i, Violation.Rule.HIERARCHY_CYCLE, "client is its own parent");
                    else if (Arrays.binarySearch(clientIds, c.parentClientId()) < 0) flag(EntityKind.CLIENT, i, Violation.Rule.MISSING_REFERENCE, "parent_client_id " + c.parentClientId() + " not in CLIENT");
                }
            }
            uniqueStrings(EntityKind.CLIENT, cl.length, i -> cl[i].ref(), null, "client_ref");
            uniqueStrings(EntityKind.CLIENT, cl.length, i -> cl[i].lei(), null, "lei");
            cycles();

            for (int i = 0; i < ad.length; i++) {
                Address a = ad[i];
                if (i > 0 && ad[i - 1].id() == a.id()) flag(EntityKind.ADDRESS, i, Violation.Rule.DUPLICATE_KEY, "duplicate address_id");
                if (a.type() == null || blank(a.line1()) || blank(a.city()) || blank(a.postalCode()) || blank(a.countryCode()))
                    flag(EntityKind.ADDRESS, i, Violation.Rule.MANDATORY_MISSING, "address_type, line1, city, postal_code, country_code are mandatory");
                if (has(EntityKind.CLIENT) && !goodClient(a.clientId())) flag(EntityKind.ADDRESS, i, Violation.Rule.MISSING_REFERENCE, "client_id " + a.clientId() + " not in CLIENT");
                if (has(EntityKind.COUNTRY) && a.countryCode() != null && !countryPos.containsKey(a.countryCode())) flag(EntityKind.ADDRESS, i, Violation.Rule.MISSING_REFERENCE, "country_code " + a.countryCode() + " not in COUNTRY");
                if (a.validTo() <= a.validFrom()) flag(EntityKind.ADDRESS, i, Violation.Rule.INVALID_DATE_RANGE, "valid_to must be after valid_from");
            }
            overlaps(EntityKind.ADDRESS, ad.length,
                    i -> { Address a = ad[i]; if (!a.primary() || a.type() == null) return -1; int p = Arrays.binarySearch(clientIds, a.clientId()); return p < 0 ? -1 : ((long) p << 8) | a.type().ordinal(); },
                    i -> ad[i].validFrom(), i -> ad[i].validTo(), i -> ad[i].id(), (i, j) -> "primary " + ad[i].type() + " address overlaps address " + ad[j].id());

            for (int i = 0; i < ac.length; i++) {
                Account a = ac[i];
                if (i > 0 && ac[i - 1].id() == a.id()) flag(EntityKind.ACCOUNT, i, Violation.Rule.DUPLICATE_KEY, "duplicate account_id");
                if (blank(a.number()) || a.type() == null || a.status() == null || blank(a.baseCurrency()) || blank(a.bookingCountry()))
                    flag(EntityKind.ACCOUNT, i, Violation.Rule.MANDATORY_MISSING, "account_number, account_type, status, base_currency, booking_country are mandatory");
                if (has(EntityKind.CLIENT) && !goodClient(a.clientId())) flag(EntityKind.ACCOUNT, i, Violation.Rule.MISSING_REFERENCE, "client_id " + a.clientId() + " not in CLIENT");
                if (has(EntityKind.CURRENCY) && a.baseCurrency() != null && !currencyPos.containsKey(a.baseCurrency())) flag(EntityKind.ACCOUNT, i, Violation.Rule.MISSING_REFERENCE, "base_currency " + a.baseCurrency() + " not in CURRENCY");
                if (has(EntityKind.COUNTRY) && a.bookingCountry() != null && !countryPos.containsKey(a.bookingCountry())) flag(EntityKind.ACCOUNT, i, Violation.Rule.MISSING_REFERENCE, "booking_country " + a.bookingCountry() + " not in COUNTRY");
                if (a.isClosed() && a.closedDate() < a.openedDate()) flag(EntityKind.ACCOUNT, i, Violation.Rule.INVALID_DATE_RANGE, "closed_date before opened_date");
            }
            uniqueStrings(EntityKind.ACCOUNT, ac.length, i -> ac[i].number(), null, "account_number");

            for (int i = 0; i < ss.length; i++) {
                SettlementInstruction s = ss[i];
                if (i > 0 && ss[i - 1].id() == s.id()) flag(EntityKind.SETTLEMENT_INSTRUCTION, i, Violation.Rule.DUPLICATE_KEY, "duplicate ssi_id");
                if (blank(s.currencyCode()) || s.assetClass() == null || blank(s.beneficiaryBic()) || blank(s.agentBic()) || blank(s.agentAccount()))
                    flag(EntityKind.SETTLEMENT_INSTRUCTION, i, Violation.Rule.MANDATORY_MISSING, "currency_code, asset_class, beneficiary_bic, agent_bic, agent_account are mandatory");
                if (has(EntityKind.ACCOUNT) && !goodAccount(s.accountId())) flag(EntityKind.SETTLEMENT_INSTRUCTION, i, Violation.Rule.MISSING_REFERENCE, "account_id " + s.accountId() + " not in ACCOUNT");
                if (has(EntityKind.CURRENCY) && s.currencyCode() != null && !currencyPos.containsKey(s.currencyCode())) flag(EntityKind.SETTLEMENT_INSTRUCTION, i, Violation.Rule.MISSING_REFERENCE, "currency_code " + s.currencyCode() + " not in CURRENCY");
                if (s.effectiveTo() <= s.effectiveFrom()) flag(EntityKind.SETTLEMENT_INSTRUCTION, i, Violation.Rule.INVALID_DATE_RANGE, "effective_to must be after effective_from");
            }
            overlaps(EntityKind.SETTLEMENT_INSTRUCTION, ss.length,
                    i -> { SettlementInstruction s = ss[i]; if (!s.isDefault() || s.assetClass() == null || s.currencyCode() == null) return -1;
                           int p = Arrays.binarySearch(accountIds, s.accountId()); Integer ccy = currencyPos.get(s.currencyCode());
                           int c = ccy == null ? (s.currencyCode().hashCode() & 0xFFFF) : ccy; return p < 0 ? -1 : ((long) p << 24) | ((long) c << 4) | s.assetClass().ordinal(); },
                    i -> ss[i].effectiveFrom(), i -> ss[i].effectiveTo(), i -> ss[i].id(), (i, j) -> "default SSI overlaps SSI " + ss[j].id());
        }

        private boolean goodClient(long id) { int p = Arrays.binarySearch(clientIds, id); return p >= 0 && !badCl[p]; }
        private boolean goodAccount(long id) { int p = Arrays.binarySearch(accountIds, id); return p >= 0 && !badAc[p]; }

        private void minRows() {
            for (Map.Entry<EntityKind, Long> e : minRows.entrySet()) {
                int n = switch (e.getKey()) { case COUNTRY -> co.length; case CURRENCY -> cu.length; case CLIENT -> cl.length;
                    case ADDRESS -> ad.length; case ACCOUNT -> ac.length; case SETTLEMENT_INSTRUCTION -> ss.length; };
                if (n < e.getValue()) { report.add(new Violation(Violation.Rule.TOO_FEW_ROWS, e.getKey(), -1, n + " rows, minimum " + e.getValue())); tableLevel = true; }
            }
        }

        /** Code -> position for the rows that are not flagged (so a reference to a bad lookup row counts as missing). */
        private static Map<String, Integer> codePositions(int n, java.util.function.IntFunction<String> code, boolean[] bad) {
            Map<String, Integer> m = new HashMap<>(n * 2);
            for (int i = 0; i < n; i++) if (!bad[i] && code.apply(i) != null) m.putIfAbsent(code.apply(i), i);
            return m;
        }

        /** Flags every row whose (non-null) string value appears more than once; O(n log n), one array of references. */
        private void uniqueStrings(EntityKind kind, int n, java.util.function.IntFunction<String> value, java.util.function.IntFunction<String> codeOf, String column) {
            Integer[] order = new Integer[n];
            int m = 0;
            for (int i = 0; i < n; i++) if (value.apply(i) != null) order[m++] = i;
            Arrays.sort(order, 0, m, Comparator.comparing(value::apply));
            for (int k = 1; k < m; k++) {
                int i = order[k];
                if (value.apply(i).equals(value.apply(order[k - 1]))) {
                    if (codeOf != null) flag(kind, i, codeOf.apply(i), Violation.Rule.DUPLICATE_KEY, "duplicate " + column + " " + value.apply(i));
                    else flag(kind, i, Violation.Rule.DUPLICATE_KEY, "duplicate " + column + " " + value.apply(i));
                }
            }
        }

        private void uniqueInts(EntityKind kind, int n, java.util.function.IntUnaryOperator value, java.util.function.IntFunction<String> codeOf, String column) {
            long[] packed = new long[n];
            for (int i = 0; i < n; i++) packed[i] = ((long) value.applyAsInt(i) << 32) | i;
            Arrays.sort(packed);
            for (int k = 1; k < n; k++) if ((packed[k] >> 32) == (packed[k - 1] >> 32))
                flag(kind, (int) packed[k], codeOf.apply((int) packed[k]), Violation.Rule.DUPLICATE_KEY, "duplicate " + column + " " + (packed[k] >> 32));
        }

        private void cycles() {
            byte[] state = new byte[cl.length];   // 0 = unvisited, 1 = on the current path, 2 = done
            int[] path = new int[64];
            for (int start = 0; start < cl.length; start++) {
                if (state[start] != 0) continue;
                int len = 0, cur = start;
                while (cur >= 0 && state[cur] == 0) {
                    state[cur] = 1;
                    if (len == path.length) path = Arrays.copyOf(path, len * 2);
                    path[len++] = cur;
                    Client c = cl[cur];
                    cur = c.hasParent() && c.parentClientId() != c.id() ? Arrays.binarySearch(clientIds, c.parentClientId()) : -1;
                }
                if (cur >= 0 && state[cur] == 1) {                    // closed a loop within this path
                    int from = 0;
                    while (path[from] != cur) from++;
                    StringBuilder ids = new StringBuilder("[");
                    for (int k = from; k < len; k++) ids.append(k > from ? ", " : "").append(cl[path[k]].id());
                    String detail = "parent chain forms a cycle: " + ids.append(']');
                    for (int k = from; k < len; k++) flag(EntityKind.CLIENT, path[k], Violation.Rule.HIERARCHY_CYCLE, detail);
                }
                for (int k = 0; k < len; k++) state[path[k]] = 2;
            }
        }

        interface GroupKey { long of(int pos); }
        interface Detail { String of(int pos, int otherPos); }

        /**
         * Flags every row whose [from, to) period overlaps an earlier period of the same group (a running maximum
         * end is kept, so a long period followed by several short ones flags each short one).
         */
        private void overlaps(EntityKind kind, int n, GroupKey key, java.util.function.IntUnaryOperator from, java.util.function.IntUnaryOperator to,
                              java.util.function.IntToLongFunction idOf, Detail detail) {
            long[] keys = new long[n];
            int m = 0;
            for (int i = 0; i < n; i++) { long k = key.of(i); if (k >= 0) keys[m++] = k; }
            if (m < 2) return;
            long[] distinct = Arrays.copyOf(keys, m);
            Arrays.sort(distinct);
            long[] sorted = new long[m];                                    // (ordinal of key << 32 | position)
            m = 0;
            for (int i = 0; i < n; i++) { long k = key.of(i); if (k >= 0) sorted[m++] = ((long) Arrays.binarySearch(distinct, k) << 32) | i; }
            Arrays.sort(sorted);
            int[] group = new int[16];
            for (int g = 0; g < m; ) {
                int end = g + 1;
                while (end < m && (sorted[end] >> 32) == (sorted[g] >> 32)) end++;
                int size = end - g;
                if (size > 1) {
                    if (group.length < size) group = new int[size];
                    for (int k = 0; k < size; k++) group[k] = (int) sorted[g + k];
                    for (int a = 1; a < size; a++) {                            // insertion sort by from, then id (groups are small)
                        int v = group[a], b = a - 1;
                        while (b >= 0 && (from.applyAsInt(group[b]) > from.applyAsInt(v) || (from.applyAsInt(group[b]) == from.applyAsInt(v) && idOf.applyAsLong(group[b]) > idOf.applyAsLong(v)))) { group[b + 1] = group[b]; b--; }
                        group[b + 1] = v;
                    }
                    int maxTo = to.applyAsInt(group[0]), maxPos = group[0];
                    for (int k = 1; k < size; k++) {
                        int pos = group[k];
                        if (from.applyAsInt(pos) < maxTo) flag(kind, pos, Violation.Rule.OVERLAPPING_PERIODS, detail.of(pos, maxPos));
                        if (to.applyAsInt(pos) > maxTo) { maxTo = to.applyAsInt(pos); maxPos = pos; }
                    }
                }
                g = end;
            }
        }

        private void flag(EntityKind kind, int pos, Violation.Rule rule, String detail) {
            long id = switch (kind) { case CLIENT -> cl[pos].id(); case ADDRESS -> ad[pos].id(); case ACCOUNT -> ac[pos].id();
                case SETTLEMENT_INSTRUCTION -> ss[pos].id(); default -> -1; };
            report.add(new Violation(rule, kind, id, detail));
            bad(kind)[pos] = true;
        }
        private void flag(EntityKind kind, int pos, String code, Violation.Rule rule, String detail) {
            report.add(new Violation(rule, kind, -1, (code == null ? "<null>" : code) + ": " + detail));
            bad(kind)[pos] = true;
        }
        private boolean[] bad(EntityKind kind) {
            return switch (kind) { case COUNTRY -> badCo; case CURRENCY -> badCu; case CLIENT -> badCl; case ADDRESS -> badAd; case ACCOUNT -> badAc; case SETTLEMENT_INSTRUCTION -> badSs; };
        }

        /** Rows of the kind that survive quarantine (rows flagged themselves; dependants of flagged rows were flagged by the reference rules). */
        <T> T[] keep(T[] rows, EntityKind kind, java.util.function.IntFunction<T[]> make) {
            boolean[] bad = bad(kind);
            List<T> out = new ArrayList<>(rows.length);
            int dropped = 0;
            for (int i = 0; i < rows.length; i++) { if (bad[i]) dropped++; else out.add(rows[i]); }
            report.quarantined(kind, dropped);
            return out.toArray(make.apply(0));
        }

        private boolean has(EntityKind k) { return loaded.contains(k); }
        private static boolean blank(String s) { return s == null || s.isBlank(); }
        private static boolean upperAlpha(String s, int len) {
            if (s == null || s.length() != len) return false;
            for (int i = 0; i < len; i++) { char c = s.charAt(i); if (c < 'A' || c > 'Z') return false; }
            return true;
        }
    }
}
