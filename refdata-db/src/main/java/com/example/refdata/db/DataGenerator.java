package com.example.refdata.db;

import com.example.refdata.model.Country;
import com.example.refdata.model.Currency;
import com.example.refdata.model.EntityKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.EnumMap;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.TimeZone;

/**
 * Deterministic generator of realistic reference data (design section 5.3). The same client count and seed always
 * produce the same rows. Shape: skewed address/account/SSI counts per parent (most have 1-3, a few have hundreds),
 * about 10% of clients in hierarchies up to four levels, 70% with an LEI, about 5% closed accounts, non-overlapping
 * effective periods for primary addresses per (client, type) and for default SSIs per (account, currency, asset
 * class), dense sequential ids from 1. Writes with JDBC batches (auto-commit off), in foreign-key order, into
 * tables created without constraints; the keys are added once the data is in.
 */
public final class DataGenerator {
    private static final Logger log = LoggerFactory.getLogger(DataGenerator.class);

    /** The fixed "today" of the generated data, so that dates do not drift between runs with the same seed. */
    public static final LocalDate DEFAULT_AS_OF = LocalDate.of(2026, 10, 1);
    /** Average children per parent the distributions are calibrated to. */
    public static final double ADDRESSES_PER_CLIENT = 2.5;
    public static final double ACCOUNTS_PER_CLIENT = 3.0;
    public static final double SSIS_PER_ACCOUNT = 1.5;
    /** The release id of the single REFDATA_RELEASE row the generator writes. */
    public static final long RELEASE_ID = 1L;

    static final int BATCH_SIZE = 1_000;
    static final int ROWS_PER_COMMIT = 50_000;

    /** Standard sizes (design 5.3). */
    public enum Profile {
        TINY(1_000), MEDIUM(100_000), LARGE(1_000_000), XLARGE(5_000_000);

        private final long clientCount;

        Profile(long clientCount) {
            this.clientCount = clientCount;
        }

        public long clientCount() {
            return clientCount;
        }
    }

    /** What a run produced. */
    public record Result(Map<EntityKind, Long> rowCounts, Duration elapsed) {
    }

    /** Expected row counts for a client count (the large tables are averages; see the per-parent distributions). */
    public static Map<EntityKind, Long> expectedRows(long clientCount) {
        Map<EntityKind, Long> m = new EnumMap<>(EntityKind.class);
        m.put(EntityKind.COUNTRY, (long) IsoCountries.all().size());
        m.put(EntityKind.CURRENCY, (long) IsoCurrencies.all().size());
        m.put(EntityKind.CLIENT, clientCount);
        m.put(EntityKind.ADDRESS, Math.round(clientCount * ADDRESSES_PER_CLIENT));
        m.put(EntityKind.ACCOUNT, Math.round(clientCount * ACCOUNTS_PER_CLIENT));
        m.put(EntityKind.SETTLEMENT_INSTRUCTION, Math.round(clientCount * ACCOUNTS_PER_CLIENT * SSIS_PER_ACCOUNT));
        return m;
    }

    private final long clientCount;
    private final long seed;
    private final LocalDate asOf;

    public DataGenerator(Profile profile, long seed) {
        this(profile.clientCount(), seed);
    }

    public DataGenerator(long clientCount, long seed) {
        this(clientCount, seed, DEFAULT_AS_OF);
    }

    public DataGenerator(long clientCount, long seed, LocalDate asOf) {
        if (clientCount < 1) throw new IllegalArgumentException("clientCount must be >= 1: " + clientCount);
        this.clientCount = clientCount;
        this.seed = seed;
        this.asOf = asOf;
    }

    /**
     * Drops any existing schema, creates the tables, fills them and adds the constraints. Runs with auto-commit off
     * (restored afterwards) and commits every {@value #ROWS_PER_COMMIT} rows; on failure the open transaction is rolled back.
     */
    public Result generate(Connection connection) throws SQLException {
        boolean autoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        try {
            return generateInTransactions(connection);
        } catch (SQLException | RuntimeException e) {
            connection.rollback();
            throw e;
        } finally {
            connection.setAutoCommit(autoCommit);
        }
    }

    private Result generateInTransactions(Connection connection) throws SQLException {
        long start = System.nanoTime();
        RefDataSchema.dropIfExists(connection);
        RefDataSchema.execute(connection, RefDataSchema.tableStatements());
        log.info("generating {} clients (seed {}) into {}", clientCount, seed, connection.getMetaData().getURL());
        Run run = new Run(connection);
        run.writeLookups();
        run.writeClientsAndChildren();
        run.writeRelease();
        connection.commit();
        log.info("rows written in {} s, adding keys and indexes", elapsedSeconds(start));
        RefDataSchema.execute(connection, RefDataSchema.constraintStatements());
        Duration elapsed = Duration.ofNanos(System.nanoTime() - start);
        log.info("done in {} s: {}", elapsed.toMillis() / 1000.0, run.counts);
        return new Result(Collections.unmodifiableMap(new EnumMap<>(run.counts)), elapsed);
    }

    private static String elapsedSeconds(long startNanos) {
        return String.format(Locale.ROOT, "%.1f", (System.nanoTime() - startNanos) / 1e9);
    }

    // ------------------------------------------------------------------------------------------------ one run

    /** The state of one generation run: random source, prepared statements, counters. */
    private final class Run {
        private static final int OPEN = Integer.MAX_VALUE;

        private final Connection connection;
        private final SplittableRandom rnd = new SplittableRandom(seed);
        private final Calendar utc = new GregorianCalendar(TimeZone.getTimeZone("UTC"), Locale.ROOT);
        private final Map<EntityKind, Long> counts = new EnumMap<>(EntityKind.class);
        private final int today;
        private final long nowMillis;
        private final List<Currency> activeCurrencies = IsoCurrencies.active();
        private final Country[] countries = IsoCountries.all().toArray(new Country[0]);
        /** Hierarchy depth per client id (index = id); 0 = top level. */
        private final byte[] depth;

        private final PreparedStatement clientStmt;
        private final PreparedStatement addressStmt;
        private final PreparedStatement accountStmt;
        private final PreparedStatement ssiStmt;
        private int clientBatch, addressBatch, accountBatch, ssiBatch;
        private long rowsSinceCommit;
        private long nextAddressId = 1, nextAccountId = 1, nextSsiId = 1;

        Run(Connection connection) throws SQLException {
            this.connection = connection;
            this.today = (int) asOf.toEpochDay();
            this.nowMillis = asOf.atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
            this.depth = new byte[(int) clientCount + 1];
            for (EntityKind k : EntityKind.values()) counts.put(k, 0L);
            clientStmt = connection.prepareStatement("INSERT INTO CLIENT (client_id, client_ref, lei, legal_name, short_name, client_type, status, "
                    + "incorporation_country, parent_client_id, risk_rating, onboarded_date, row_version, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)");
            addressStmt = connection.prepareStatement("INSERT INTO ADDRESS (address_id, client_id, address_type, line1, line2, line3, city, region, "
                    + "postal_code, country_code, is_primary, valid_from, valid_to, row_version, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
            accountStmt = connection.prepareStatement("INSERT INTO ACCOUNT (account_id, account_number, client_id, account_type, base_currency, "
                    + "booking_country, status, opened_date, closed_date, row_version, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)");
            ssiStmt = connection.prepareStatement("INSERT INTO SETTLEMENT_INSTRUCTION (ssi_id, account_id, currency_code, asset_class, beneficiary_bic, "
                    + "agent_bic, agent_account, is_default, effective_from, effective_to, row_version, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)");
        }

        // ---- lookups

        void writeLookups() throws SQLException {
            try (PreparedStatement ps = connection.prepareStatement("INSERT INTO COUNTRY (country_code, iso3, numeric_code, name, region) VALUES (?,?,?,?,?)")) {
                for (Country c : countries) {
                    ps.setString(1, c.code()); ps.setString(2, c.iso3()); ps.setInt(3, c.numericCode());
                    ps.setString(4, c.name()); ps.setString(5, c.region());
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            counts.put(EntityKind.COUNTRY, (long) countries.length);
            List<Currency> currencies = IsoCurrencies.all();
            try (PreparedStatement ps = connection.prepareStatement("INSERT INTO CURRENCY (currency_code, numeric_code, name, minor_units, active) VALUES (?,?,?,?,?)")) {
                for (Currency c : currencies) {
                    ps.setString(1, c.code()); ps.setInt(2, c.numericCode()); ps.setString(3, c.name());
                    ps.setInt(4, c.minorUnits()); ps.setBoolean(5, c.active());
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            counts.put(EntityKind.CURRENCY, (long) currencies.size());
            connection.commit();
        }

        void writeRelease() throws SQLException {
            try (PreparedStatement ps = connection.prepareStatement("INSERT INTO REFDATA_RELEASE (release_id, status, published_at, description) VALUES (?,?,?,?)")) {
                ps.setLong(1, RELEASE_ID);
                ps.setString(2, "PUBLISHED");
                ps.setTimestamp(3, new Timestamp(nowMillis), utc);
                ps.setString(4, "generated: " + clientCount + " clients, seed " + seed);
                ps.executeUpdate();
            }
        }

        // ---- clients and everything under them

        void writeClientsAndChildren() throws SQLException {
            long logEvery = Math.max(10_000, clientCount / 20);
            for (long id = 1; id <= clientCount; id++) {
                writeClient(id);
                if (id % logEvery == 0 || id == clientCount) {
                    log.info("{} / {} clients ({} rows so far)", id, clientCount, totalRows());
                }
            }
            flushAll();
            connection.commit();
        }

        private void writeClient(long id) throws SQLException {
            String type = pick(CLIENT_TYPES, CLIENT_TYPE_WEIGHTS);
            Country country = pickCountry();
            String legalName = "INDIVIDUAL".equals(type) ? pick(FIRST_NAMES) + " " + pick(SURNAMES) : companyName(type);
            String shortName = legalName.length() <= 50 ? legalName : legalName.substring(0, 50).trim();
            long parent = chooseParent(id);
            int onboarded = today - between(30, 20 * 365);
            String status = pick(CLIENT_STATUSES, CLIENT_STATUS_WEIGHTS);

            clientStmt.setLong(1, id);
            clientStmt.setString(2, String.format(Locale.ROOT, "C%09d", id));
            clientStmt.setString(3, chance(0.70) ? lei(id) : null);
            clientStmt.setString(4, legalName);
            clientStmt.setString(5, shortName);
            clientStmt.setString(6, type);
            clientStmt.setString(7, status);
            clientStmt.setString(8, country.code());
            if (parent == 0) clientStmt.setNull(9, java.sql.Types.BIGINT); else clientStmt.setLong(9, parent);
            clientStmt.setInt(10, 1 + rnd.nextInt(9));
            setDate(clientStmt, 11, onboarded);
            clientStmt.setInt(12, rowVersion());
            setUpdatedAt(clientStmt, 13);
            clientStmt.addBatch();
            clientBatch++;
            counted(EntityKind.CLIENT);

            writeAddresses(id, country, onboarded);
            writeAccounts(id, country, onboarded);
            flushIfFull();
        }

        /** ~7% of clients get a parent among the preceding clients, never deeper than level 3 (four levels). */
        private long chooseParent(long id) {
            if (id == 1 || !chance(0.07)) return 0;
            long low = Math.max(1, id - 2_000);
            long parent = low + rnd.nextLong(id - low);
            if (depth[(int) parent] >= 3) return 0;
            depth[(int) id] = (byte) (depth[(int) parent] + 1);
            return parent;
        }

        private void writeAddresses(long clientId, Country clientCountry, int onboarded) throws SQLException {
            int n = addressesPerClient();
            int[] chainEnd = new int[ADDRESS_TYPES.length];      // 0 = no primary chain yet, OPEN = chain ended open
            for (int i = 0; i < n; i++) {
                int typeIx = i == 0 ? 0 : pickIndex(ADDRESS_TYPE_WEIGHTS);
                boolean primary;
                int from, to;
                if (chainEnd[typeIx] == OPEN) {                    // a current primary exists: this one is secondary
                    primary = false;
                    from = onboarded + rnd.nextInt(Math.max(1, today - onboarded));
                    to = chance(0.5) ? OPEN : from + between(30, 1_500);
                } else {
                    primary = true;
                    from = chainEnd[typeIx] == 0 ? onboarded : chainEnd[typeIx];
                    to = chance(0.6) ? OPEN : from + between(90, 2_000);
                    chainEnd[typeIx] = to;
                }
                City city = pickCity(clientCountry);
                addressStmt.setLong(1, nextAddressId++);
                addressStmt.setLong(2, clientId);
                addressStmt.setString(3, ADDRESS_TYPES[typeIx]);
                addressStmt.setString(4, (1 + rnd.nextInt(250)) + " " + pick(STREETS));
                addressStmt.setString(5, chance(0.2) ? pick(LINE2_PREFIX) + " " + (1 + rnd.nextInt(40)) : null);
                addressStmt.setString(6, chance(0.05) ? "PO Box " + (100 + rnd.nextInt(9_900)) : null);
                addressStmt.setString(7, city.name);
                addressStmt.setString(8, city.region);
                addressStmt.setString(9, postalCode(city.country));
                addressStmt.setString(10, city.country);
                addressStmt.setBoolean(11, primary);
                setDate(addressStmt, 12, from);
                setNullableDate(addressStmt, 13, to);
                addressStmt.setInt(14, rowVersion());
                setUpdatedAt(addressStmt, 15);
                addressStmt.addBatch();
                addressBatch++;
                counted(EntityKind.ADDRESS);
            }
        }

        private void writeAccounts(long clientId, Country clientCountry, int onboarded) throws SQLException {
            int n = accountsPerClient();
            for (int i = 0; i < n; i++) {
                long accountId = nextAccountId++;
                Currency base = pickCurrency();
                String booking = chance(0.7) ? clientCountry.code() : pickCountry().code();
                int opened = onboarded + rnd.nextInt(Math.max(1, today - onboarded));
                boolean closed = chance(0.05);
                int closedDate = closed ? opened + rnd.nextInt(Math.max(1, today - opened)) : OPEN;
                String status = closed ? "CLOSED" : pick(ACCOUNT_STATUSES, ACCOUNT_STATUS_WEIGHTS);

                accountStmt.setLong(1, accountId);
                accountStmt.setString(2, accountNumber(booking, accountId));
                accountStmt.setLong(3, clientId);
                accountStmt.setString(4, pick(ACCOUNT_TYPES, ACCOUNT_TYPE_WEIGHTS));
                accountStmt.setString(5, base.code());
                accountStmt.setString(6, booking);
                accountStmt.setString(7, status);
                setDate(accountStmt, 8, opened);
                setNullableDate(accountStmt, 9, closedDate);
                accountStmt.setInt(10, rowVersion());
                setUpdatedAt(accountStmt, 11);
                accountStmt.addBatch();
                accountBatch++;
                counted(EntityKind.ACCOUNT);

                writeSsis(accountId, base, opened);
            }
        }

        /** One effective-dated chain per (currency, asset class); a chain that reached "open-ended" only gets non-default extras. */
        private void writeSsis(long accountId, Currency base, int opened) throws SQLException {
            int n = pickIndex(SSI_COUNT_WEIGHTS);
            List<SsiChain> chains = new ArrayList<>(4);
            for (int i = 0; i < n; i++) {
                SsiChain chain;
                if (i == 0) chain = newChain(chains, base.code(), "CASH", opened);
                else if (chance(0.5)) chain = chains.get(rnd.nextInt(chains.size()));
                else chain = newChain(chains, pickCurrency().code(), pick(ASSET_CLASSES), opened);

                boolean isDefault;
                int from, to;
                if (chain.lastTo == OPEN) {
                    isDefault = false;
                    from = chain.lastFrom + between(1, 365);
                    to = chance(0.5) ? OPEN : from + between(30, 700);
                } else {
                    isDefault = chance(0.85);
                    from = chain.lastTo;
                    to = chance(0.6) ? OPEN : from + between(90, 1_500);
                    chain.lastTo = isDefault ? to : from + 1;   // a non-default row does not block the next default
                }
                chain.lastFrom = from;

                ssiStmt.setLong(1, nextSsiId++);
                ssiStmt.setLong(2, accountId);
                ssiStmt.setString(3, chain.currency);
                ssiStmt.setString(4, chain.assetClass);
                ssiStmt.setString(5, bic());
                ssiStmt.setString(6, bic());
                ssiStmt.setString(7, agentAccount());
                ssiStmt.setBoolean(8, isDefault);
                setDate(ssiStmt, 9, from);
                setNullableDate(ssiStmt, 10, to);
                ssiStmt.setInt(11, rowVersion());
                setUpdatedAt(ssiStmt, 12);
                ssiStmt.addBatch();
                ssiBatch++;
                counted(EntityKind.SETTLEMENT_INSTRUCTION);
            }
        }

        private SsiChain newChain(List<SsiChain> chains, String currency, String assetClass, int opened) {
            for (SsiChain c : chains) if (c.currency.equals(currency) && c.assetClass.equals(assetClass)) return c;
            SsiChain c = new SsiChain(currency, assetClass, opened);
            chains.add(c);
            return c;
        }

        private static final class SsiChain {
            final String currency;
            final String assetClass;
            int lastFrom;
            /** Where the next default instruction may start; OPEN once a default row is open-ended. */
            int lastTo;

            SsiChain(String currency, String assetClass, int start) {
                this.currency = currency;
                this.assetClass = assetClass;
                this.lastFrom = start;
                this.lastTo = start;
            }
        }

        // ---- batching

        private void counted(EntityKind kind) {
            counts.merge(kind, 1L, Long::sum);
            rowsSinceCommit++;
        }

        private long totalRows() {
            long t = 0;
            for (long v : counts.values()) t += v;
            return t;
        }

        private void flushIfFull() throws SQLException {
            if (clientBatch >= BATCH_SIZE || addressBatch >= BATCH_SIZE || accountBatch >= BATCH_SIZE || ssiBatch >= BATCH_SIZE) {
                flushAll();
                if (rowsSinceCommit >= ROWS_PER_COMMIT) {
                    connection.commit();
                    rowsSinceCommit = 0;
                }
            }
        }

        /** Executes the pending batches in foreign-key order (parents before children). */
        private void flushAll() throws SQLException {
            if (clientBatch > 0) { clientStmt.executeBatch(); clientBatch = 0; }
            if (addressBatch > 0) { addressStmt.executeBatch(); addressBatch = 0; }
            if (accountBatch > 0) { accountStmt.executeBatch(); accountBatch = 0; }
            if (ssiBatch > 0) { ssiStmt.executeBatch(); ssiBatch = 0; }
        }

        // ---- value helpers

        private void setDate(PreparedStatement ps, int index, int epochDay) throws SQLException {
            ps.setDate(index, new java.sql.Date(epochDay * 86_400_000L), utc);
        }

        private void setNullableDate(PreparedStatement ps, int index, int epochDay) throws SQLException {
            if (epochDay == OPEN) ps.setNull(index, java.sql.Types.DATE); else setDate(ps, index, epochDay);
        }

        private void setUpdatedAt(PreparedStatement ps, int index) throws SQLException {
            long millis = nowMillis - rnd.nextLong(3L * 365 * 86_400_000L);
            ps.setTimestamp(index, new Timestamp(millis - millis % 1000), utc);
        }

        private int rowVersion() {
            return chance(0.6) ? 1 : 2 + rnd.nextInt(30);
        }

        private boolean chance(double p) {
            return rnd.nextDouble() < p;
        }

        private int between(int lowInclusive, int highInclusive) {
            return lowInclusive + rnd.nextInt(highInclusive - lowInclusive + 1);
        }

        private <T> T pick(T[] values) {
            return values[rnd.nextInt(values.length)];
        }

        private <T> T pick(T[] values, int[] weights) {
            return values[pickIndex(weights)];
        }

        /** Index drawn with the given relative weights. */
        private int pickIndex(int[] weights) {
            int total = 0;
            for (int w : weights) total += w;
            int r = rnd.nextInt(total);
            for (int i = 0; i < weights.length; i++) {
                r -= weights[i];
                if (r < 0) return i;
            }
            return weights.length - 1;
        }

        /** Mean 2.5: 36% one, 34% two, 18% three, most of the rest 4-9, one in a thousand 100-300. */
        private int addressesPerClient() {
            double r = rnd.nextDouble();
            if (r < 0.36) return 1;
            if (r < 0.70) return 2;
            if (r < 0.88) return 3;
            if (r < 0.999) return between(4, 9);
            return between(100, 300);
        }

        /** Mean 3.0: 32% one, 32% two, 20% three, most of the rest 4-9, two in a thousand 100-400. */
        private int accountsPerClient() {
            double r = rnd.nextDouble();
            if (r < 0.32) return 1;
            if (r < 0.64) return 2;
            if (r < 0.84) return 3;
            if (r < 0.998) return between(4, 9);
            return between(100, 400);
        }

        private Country pickCountry() {
            return chance(0.75) ? countries[MAJOR_COUNTRY_INDEX[rnd.nextInt(MAJOR_COUNTRY_INDEX.length)]] : pick(countries);
        }

        private Currency pickCurrency() {
            return chance(0.8) ? activeCurrencies.get(MAJOR_CURRENCY_INDEX[rnd.nextInt(MAJOR_CURRENCY_INDEX.length)]) : activeCurrencies.get(rnd.nextInt(activeCurrencies.size()));
        }

        private City pickCity(Country clientCountry) {
            if (chance(0.8)) {
                City[] local = CITIES_BY_COUNTRY.get(clientCountry.code());
                if (local != null) return pick(local);
            }
            return pick(CITIES);
        }

        private String companyName(String type) {
            String stem = chance(0.5) ? pick(SURNAMES) : pick(PLACE_WORDS);
            return switch (type) {
                case "FUND" -> stem + " " + pick(FUND_WORDS) + " " + pick(FUND_SUFFIXES);
                case "BANK" -> pick(BANK_PATTERNS).replace("%", stem);
                case "GOVERNMENT" -> pick(GOVERNMENT_PATTERNS).replace("%", stem);
                default -> stem + " " + pick(INDUSTRY_WORDS) + " " + pick(CORPORATE_SUFFIXES);
            };
        }

        /** 20 characters: 4-char LOU prefix, two reserved zeros, the id in base 36 (12 chars), two check digits. */
        private String lei(long id) {
            String body = Long.toString(id, 36).toUpperCase(Locale.ROOT);
            return pick(LOU_PREFIXES) + "00" + "0".repeat(12 - body.length()) + body + (10 + rnd.nextInt(89));
        }

        /** IBAN-like: country, 2 check digits, 4-letter bank code, 14-digit number carrying the id (unique). */
        private String accountNumber(String country, long id) {
            return country + String.format(Locale.ROOT, "%02d", 10 + rnd.nextInt(89)) + pick(BANK_CODES) + String.format(Locale.ROOT, "%014d", id);
        }

        private String bic() {
            String branch = chance(0.5) ? "XXX" : alnum(3);
            return pick(BIC8) + branch;
        }

        private String agentAccount() {
            return chance(0.5) ? pick(IBAN_COUNTRIES) + String.format(Locale.ROOT, "%02d", 10 + rnd.nextInt(89)) + pick(BANK_CODES) + digits(10 + rnd.nextInt(9))
                    : digits(8 + rnd.nextInt(8));
        }

        private String postalCode(String country) {
            return switch (country) {
                case "GB", "IE" -> alpha(2) + (1 + rnd.nextInt(99)) + " " + (1 + rnd.nextInt(9)) + alpha(2);
                case "US" -> digits(5);
                case "CA" -> alpha(1) + digits(1) + alpha(1) + " " + digits(1) + alpha(1) + digits(1);
                case "NL" -> digits(4) + " " + alpha(2);
                case "JP" -> digits(3) + "-" + digits(4);
                case "BR" -> digits(5) + "-" + digits(3);
                default -> digits(4 + rnd.nextInt(2));
            };
        }

        private String digits(int n) {
            StringBuilder sb = new StringBuilder(n);
            for (int i = 0; i < n; i++) sb.append((char) ('0' + rnd.nextInt(10)));
            return sb.toString();
        }

        private String alpha(int n) {
            StringBuilder sb = new StringBuilder(n);
            for (int i = 0; i < n; i++) sb.append((char) ('A' + rnd.nextInt(26)));
            return sb.toString();
        }

        private String alnum(int n) {
            StringBuilder sb = new StringBuilder(n);
            for (int i = 0; i < n; i++) {
                int k = rnd.nextInt(36);
                sb.append((char) (k < 10 ? '0' + k : 'A' + k - 10));
            }
            return sb.toString();
        }
    }

    // ------------------------------------------------------------------------------------------------ vocabularies

    private static final String[] CLIENT_TYPES = {"CORPORATE", "FUND", "BANK", "INDIVIDUAL", "GOVERNMENT"};
    private static final int[] CLIENT_TYPE_WEIGHTS = {55, 20, 8, 12, 5};
    private static final String[] CLIENT_STATUSES = {"ACTIVE", "SUSPENDED", "CLOSED"};
    private static final int[] CLIENT_STATUS_WEIGHTS = {92, 3, 5};
    private static final String[] ADDRESS_TYPES = {"REGISTERED", "MAILING", "BILLING", "OPERATIONAL"};
    private static final int[] ADDRESS_TYPE_WEIGHTS = {20, 35, 25, 20};
    private static final String[] ACCOUNT_TYPES = {"CASH", "CUSTODY", "TRADING", "MARGIN"};
    private static final int[] ACCOUNT_TYPE_WEIGHTS = {45, 30, 20, 5};
    private static final String[] ACCOUNT_STATUSES = {"ACTIVE", "SUSPENDED"};
    private static final int[] ACCOUNT_STATUS_WEIGHTS = {97, 3};
    private static final String[] ASSET_CLASSES = {"CASH", "EQUITY", "FIXED_INCOME"};
    /** Instructions per account: 0..4 with mean 1.5. */
    private static final int[] SSI_COUNT_WEIGHTS = {20, 35, 25, 15, 5};

    private static final String[] MAJOR_COUNTRIES = {"GB", "US", "DE", "FR", "CH", "NL", "IE", "LU", "JP", "HK", "SG", "AU", "CA", "IT", "ES", "SE", "KY", "JE", "BE", "DK"};
    private static final int[] MAJOR_COUNTRY_INDEX = indexOfCountries(MAJOR_COUNTRIES);
    private static final String[] MAJOR_CURRENCIES = {"USD", "EUR", "GBP", "JPY", "CHF", "AUD", "CAD", "HKD", "SGD", "SEK", "DKK", "NOK"};
    private static final int[] MAJOR_CURRENCY_INDEX = indexOfCurrencies(MAJOR_CURRENCIES);

    private static int[] indexOfCountries(String[] codes) {
        List<Country> all = IsoCountries.all();
        int[] ix = new int[codes.length];
        for (int i = 0; i < codes.length; i++) {
            for (int j = 0; j < all.size(); j++) if (all.get(j).code().equals(codes[i])) ix[i] = j;
        }
        return ix;
    }

    private static int[] indexOfCurrencies(String[] codes) {
        List<Currency> all = IsoCurrencies.active();
        int[] ix = new int[codes.length];
        for (int i = 0; i < codes.length; i++) {
            for (int j = 0; j < all.size(); j++) if (all.get(j).code().equals(codes[i])) ix[i] = j;
        }
        return ix;
    }

    private static final String[] FIRST_NAMES = {"James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", "William", "Elizabeth",
        "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Sarah", "Charles", "Karen", "Hans", "Anna", "Pierre", "Marie",
        "Luca", "Giulia", "Carlos", "Sofia", "Yuki", "Haruto", "Wei", "Mei", "Ahmed", "Fatima", "Lars", "Ingrid", "Sven", "Freya", "Liam", "Olivia"};
    private static final String[] SURNAMES = {"Smith", "Johnson", "Williams", "Brown", "Jones", "Miller", "Davis", "Wilson", "Anderson", "Taylor",
        "Thomas", "Moore", "Martin", "Thompson", "White", "Harris", "Clark", "Lewis", "Robinson", "Walker", "Hartwell", "Ashford", "Pemberton",
        "Whitfield", "Lindqvist", "Berger", "Fischer", "Weber", "Schneider", "Meyer", "Rossi", "Russo", "Ferrari", "Bianchi", "Garcia", "Martinez",
        "Lopez", "Sanchez", "Dubois", "Moreau", "Laurent", "Lefebvre", "Tanaka", "Suzuki", "Sato", "Watanabe", "Chen", "Wang", "Zhang", "Li"};
    private static final String[] PLACE_WORDS = {"Northbridge", "Atlantic", "Pacific", "Meridian", "Summit", "Harbour", "Crescent", "Granite",
        "Silverline", "Oakridge", "Thames", "Rhine", "Alpine", "Baltic", "Nordic", "Iberian", "Continental", "Global", "Eastgate", "Westfield"};
    private static final String[] INDUSTRY_WORDS = {"Capital", "Industries", "Logistics", "Energy", "Technologies", "Pharmaceuticals", "Shipping",
        "Trading", "Engineering", "Properties", "Holdings", "Retail", "Media", "Chemicals", "Foods", "Mining", "Aerospace", "Textiles", "Insurance", "Motors"};
    private static final String[] CORPORATE_SUFFIXES = {"Ltd", "Limited", "plc", "GmbH", "AG", "SA", "S.A.", "SpA", "BV", "NV", "Inc.", "Corp.", "LLC", "Pty Ltd", "KK", "AB", "A/S", "Oy", "S.à r.l.", "LLP"};
    private static final String[] FUND_WORDS = {"Global Equity", "Emerging Markets", "Fixed Income", "Balanced", "Opportunities", "Income", "Growth",
        "Multi-Asset", "High Yield", "Infrastructure", "Real Estate", "Credit", "Macro", "Absolute Return", "Sustainable"};
    private static final String[] FUND_SUFFIXES = {"Fund", "Fund plc", "SICAV", "Master Fund LP", "Trust", "UCITS ETF", "Fund II LP", "FCP"};
    private static final String[] BANK_PATTERNS = {"Bank of %", "% Bank plc", "% Banking Corporation", "% Savings Bank", "Banque %", "% Bank AG", "% Trust Company", "% Merchant Bank Ltd"};
    private static final String[] GOVERNMENT_PATTERNS = {"Ministry of Finance of %", "% Treasury", "% Central Bank", "% Sovereign Wealth Fund", "City of %", "% Pension Reserve", "% Export Credit Agency", "State of %"};
    private static final String[] LOU_PREFIXES = {"5493", "2138", "9695", "7245", "5299", "8945", "3157", "2549", "6354", "9845"};
    private static final String[] BANK_CODES = {"NWBK", "BARC", "HSBC", "DEUT", "CHAS", "CITI", "BNPA", "SOGE", "UBSW", "INGB", "RABO", "NDEA", "UNCR", "BSCH", "SMBC", "ANZB", "BOFA", "CRES", "DNBA", "DABA"};
    private static final String[] BIC8 = {"DEUTDEFF", "CHASUS33", "BARCGB22", "BNPAFRPP", "HSBCHKHH", "CITIUS33", "UBSWCHZH", "INGBNL2A", "SOGEFRPP",
        "MIDLGB22", "NWBKGB2L", "COBADEFF", "BOFAUS3N", "RABONL2U", "DABADKKK", "NDEASESS", "UNCRITMM", "BSCHESMM", "SMBCJPJT", "BOTKJPJT",
        "ANZBAU3M", "RZBAATWW", "CRESCHZZ", "DNBANOKK", "BKCHCNBJ", "ICBKCNBJ", "DBSSSGSG", "OCBCSGSG", "ROYCCAT2", "TDOMCATT"};
    private static final String[] IBAN_COUNTRIES = {"GB", "DE", "FR", "NL", "CH", "IT", "ES", "BE", "LU", "IE", "AT", "SE", "DK", "NO", "FI", "PT"};
    private static final String[] STREETS = {"High Street", "Station Road", "Main Street", "Church Lane", "Park Avenue", "Victoria Street", "King Street",
        "Queen Street", "Market Square", "Mill Lane", "Bahnhofstrasse", "Hauptstrasse", "Schillerstrasse", "Rue de la Paix", "Avenue des Champs",
        "Boulevard Haussmann", "Via Roma", "Corso Italia", "Calle Mayor", "Gran Via", "Herengracht", "Keizersgracht", "Broadway", "Fifth Avenue",
        "Wall Street", "Market Street", "Lake Shore Drive", "Collins Street", "George Street", "Queen's Road", "Orchard Road", "Marunouchi",
        "Nevsky Prospekt", "Kungsgatan", "Strandvejen", "Karl Johans gate", "Mannerheimintie", "Avenida Paulista", "Paseo de la Reforma", "Bay Street"};
    private static final String[] LINE2_PREFIX = {"Floor", "Suite", "Unit", "Level", "Building", "Block", "Office"};

    /** A city with its region and country (ISO alpha-2). */
    private record City(String name, String region, String country) {
    }

    private static final City[] CITIES = {
        new City("London", "Greater London", "GB"), new City("Manchester", "Greater Manchester", "GB"), new City("Edinburgh", "Scotland", "GB"),
        new City("Birmingham", "West Midlands", "GB"), new City("Leeds", "West Yorkshire", "GB"), new City("Bristol", "South West", "GB"),
        new City("New York", "NY", "US"), new City("Chicago", "IL", "US"), new City("San Francisco", "CA", "US"), new City("Boston", "MA", "US"),
        new City("Houston", "TX", "US"), new City("Charlotte", "NC", "US"), new City("Wilmington", "DE", "US"), new City("Seattle", "WA", "US"),
        new City("Frankfurt am Main", "Hessen", "DE"), new City("Munich", "Bayern", "DE"), new City("Berlin", "Berlin", "DE"), new City("Hamburg", "Hamburg", "DE"),
        new City("Dusseldorf", "Nordrhein-Westfalen", "DE"), new City("Stuttgart", "Baden-Wurttemberg", "DE"),
        new City("Paris", "Ile-de-France", "FR"), new City("Lyon", "Auvergne-Rhone-Alpes", "FR"), new City("Marseille", "Provence-Alpes-Cote d'Azur", "FR"),
        new City("Zurich", "ZH", "CH"), new City("Geneva", "GE", "CH"), new City("Basel", "BS", "CH"), new City("Lugano", "TI", "CH"),
        new City("Amsterdam", "Noord-Holland", "NL"), new City("Rotterdam", "Zuid-Holland", "NL"), new City("The Hague", "Zuid-Holland", "NL"),
        new City("Dublin", "Leinster", "IE"), new City("Cork", "Munster", "IE"), new City("Luxembourg", null, "LU"),
        new City("Tokyo", "Tokyo", "JP"), new City("Osaka", "Osaka", "JP"), new City("Hong Kong", null, "HK"), new City("Singapore", null, "SG"),
        new City("Sydney", "NSW", "AU"), new City("Melbourne", "VIC", "AU"), new City("Toronto", "ON", "CA"), new City("Montreal", "QC", "CA"),
        new City("Milan", "Lombardia", "IT"), new City("Rome", "Lazio", "IT"), new City("Turin", "Piemonte", "IT"),
        new City("Madrid", "Comunidad de Madrid", "ES"), new City("Barcelona", "Cataluna", "ES"), new City("Stockholm", "Stockholms lan", "SE"),
        new City("Gothenburg", "Vastra Gotaland", "SE"), new City("George Town", null, "KY"), new City("St Helier", null, "JE"),
        new City("Brussels", "Brussels-Capital", "BE"), new City("Antwerp", "Flanders", "BE"), new City("Copenhagen", "Hovedstaden", "DK"),
        new City("Oslo", "Oslo", "NO"), new City("Helsinki", "Uusimaa", "FI"), new City("Vienna", "Wien", "AT"), new City("Warsaw", "Mazowieckie", "PL"),
        new City("Prague", "Praha", "CZ"), new City("Lisbon", "Lisboa", "PT"), new City("Sao Paulo", "SP", "BR"), new City("Mexico City", "CDMX", "MX"),
        new City("Mumbai", "Maharashtra", "IN"), new City("Shanghai", "Shanghai", "CN"), new City("Seoul", "Seoul", "KR"), new City("Dubai", "Dubai", "AE"),
        new City("Johannesburg", "Gauteng", "ZA"), new City("Auckland", "Auckland", "NZ"), new City("Buenos Aires", "CABA", "AR"),
    };
    private static final Map<String, City[]> CITIES_BY_COUNTRY = groupCities();

    private static Map<String, City[]> groupCities() {
        Map<String, List<City>> lists = new java.util.HashMap<>();
        for (City c : CITIES) lists.computeIfAbsent(c.country(), k -> new ArrayList<>()).add(c);
        Map<String, City[]> out = new java.util.HashMap<>();
        lists.forEach((k, v) -> out.put(k, v.toArray(new City[0])));
        return out;
    }
}
