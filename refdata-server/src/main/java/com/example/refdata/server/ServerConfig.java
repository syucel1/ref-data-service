package com.example.refdata.server;

import com.example.refdata.model.inspection.InspectionServer;

import com.example.refdata.contract.Framing;
import com.example.refdata.model.EntityKind;
import com.example.refdata.model.validation.ValidationPolicy;
import com.example.refdata.protocol.v1.Codec;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

/**
 * The server's settings: every {@code rds.*} key of DESIGN.md section 16 with its default, read from an optional
 * properties file and overridable with {@code -Drds.*} system properties (which win). Cross-rules between keys are
 * checked once in {@link #validate()}; a broken rule throws {@link ConfigException} naming the key.
 * Durations accept {@code 500ms}, {@code 30s}, {@code 10min}, {@code 1h}; a bare number is milliseconds.
 * Sizes accept {@code 64k}/{@code 64KiB}, {@code 8m}/{@code 8MiB}; a bare number is bytes.
 */
public final class ServerConfig {
    /** Prefix of every key. */
    public static final String PREFIX = "rds.";
    /** Upper bound of {@code rds.live.history.batches}: each retained batch may hold up to 512 KiB of payload. */
    public static final int MAX_LIVE_HISTORY_BATCHES = 1024;

    /** Thrown for a missing, malformed or inconsistent setting; the message names the key. */
    public static final class ConfigException extends RuntimeException {
        public ConfigException(String message) { super(message); }
    }

    private final Properties props;

    public final int tcpPort;
    public final String bindAddress;
    /** Socket send buffer in bytes; 0 = unset (kernel auto-tuning). */
    public final int tcpSendBuffer;
    public final int ioThreads;
    public final int httpPort;
    public final boolean inspectionEnabled;
    public final int adminPort;
    public final String adminToken;
    public final InspectionServer.Options inspectionOptions;
    public final String dbUrl;
    public final String dbUser;
    private final String dbPassword;
    public final int dbConnectRetries;
    public final int loadFetchSize;
    public final int loadParallelism;
    public final int loadRetries;
    public final ValidationPolicy validationPolicy;
    public final Map<EntityKind, Long> tableMinRows;
    public final double indexDenseThreshold;
    public final int chunkTargetBytes;
    public final int frameMaxBytes;
    /** Codecs pre-encoded besides NONE, in the order of the setting. */
    public final Set<Codec> codecs;
    public final int maxConnections;
    public final int maxConcurrentSnapshots;
    public final int snapshotQueueSize;
    public final Duration snapshotQueueMaxWait;
    public final int writeBufferHighWater;
    public final int writeBufferLowWater;
    public final Duration handshakeTimeout;
    public final Duration sessionIdleTimeout;
    public final Duration stallTimeout;
    public final Duration snapshotMaxDuration;
    public final Duration heartbeatInterval;
    public final Duration shutdownDrainTimeout;
    /** Published update batches kept for live consumers that catch up or reconnect and resume; older ones reload a snapshot. */
    public final int liveHistoryBatches;

    private ServerConfig(Properties props) {
        this.props = props;
        tcpPort = intValue("tcp.port", 7400);
        adminPort = intValue("admin.port", -1);
        adminToken = string("admin.token", "");
        range("admin.port", adminPort, -1, 65535);
        if (adminPort >= 0 && adminToken.length() < 16) throw new ConfigException("rds.admin.token must contain at least 16 characters");
        bindAddress = string("tcp.bind.address", "0.0.0.0");
        tcpSendBuffer = (int) size("tcp.send.buffer", 0);
        ioThreads = intValue("io.threads", Runtime.getRuntime().availableProcessors());
        httpPort = intValue("http.port", 7401);
        String enabled = string("inspection.enabled", "false");
        if (!enabled.equalsIgnoreCase("true") && !enabled.equalsIgnoreCase("false"))
            throw new ConfigException("rds.inspection.enabled must be true or false");
        inspectionEnabled = Boolean.parseBoolean(enabled);
        try {
            inspectionOptions = new InspectionServer.Options(
                string("inspection.host", "127.0.0.1"), intValue("inspection.port", 7402), intValue("inspection.max.page.size", 100));
        } catch (IllegalArgumentException e) { throw new ConfigException("rds.inspection: " + e.getMessage()); }
        dbUrl = string("db.url", "");
        dbUser = string("db.user", "");
        dbPassword = string("db.password", "");
        dbConnectRetries = intValue("db.connect.retries", 5);
        loadFetchSize = intValue("load.fetch.size", 10_000);
        loadParallelism = intValue("load.parallelism", 4);
        loadRetries = intValue("load.retries", 2);
        validationPolicy = enumValue("validation.policy", ValidationPolicy.class, ValidationPolicy.FAIL);
        Map<EntityKind, Long> mins = new EnumMap<>(EntityKind.class);
        for (EntityKind k : EntityKind.values()) mins.put(k, longValue("table.min.rows." + k.tableName().toLowerCase(Locale.ROOT), 1L));
        tableMinRows = Collections.unmodifiableMap(mins);
        indexDenseThreshold = doubleValue("index.dense.threshold", 1.5);
        chunkTargetBytes = (int) size("chunk.target.bytes", 256 << 10);
        frameMaxBytes = (int) size("frame.max.bytes", 1 << 20);
        codecs = codecs("codecs");
        maxConnections = intValue("max.connections", 256);
        maxConcurrentSnapshots = intValue("max.concurrent.snapshots", 8);
        snapshotQueueSize = intValue("snapshot.queue.size", 64);
        snapshotQueueMaxWait = duration("snapshot.queue.max.wait", Duration.ofSeconds(60));
        writeBufferHighWater = (int) size("write.buffer.high.water", 8 << 20);
        writeBufferLowWater = (int) size("write.buffer.low.water", 4 << 20);
        handshakeTimeout = duration("handshake.timeout", Duration.ofSeconds(10));
        sessionIdleTimeout = duration("session.idle.timeout", Duration.ofSeconds(60));
        stallTimeout = duration("stall.timeout", Duration.ofSeconds(30));
        snapshotMaxDuration = duration("snapshot.max.duration", Duration.ofMinutes(10));
        heartbeatInterval = duration("heartbeat.interval", Duration.ofSeconds(5));
        shutdownDrainTimeout = duration("shutdown.drain.timeout", Duration.ofSeconds(30));
        liveHistoryBatches = intValue("live.history.batches", 32);
    }

    /** Defaults, overlaid with the {@code rds.*} system properties. */
    public static ServerConfig load() {
        return fromProperties(new Properties());
    }

    /** The properties file (every {@code rds.*} key is optional), overlaid with the {@code rds.*} system properties. */
    public static ServerConfig load(Path file) {
        Properties p = new Properties();
        try (InputStream in = Files.newInputStream(file)) {
            p.load(in);
        } catch (IOException e) {
            throw new ConfigException("cannot read configuration file " + file + ": " + e.getMessage());
        }
        return fromProperties(p);
    }

    /** The given properties overlaid with the {@code rds.*} system properties, validated. */
    public static ServerConfig fromProperties(Properties base) {
        Properties merged = new Properties();
        merged.putAll(base);
        for (String name : System.getProperties().stringPropertyNames()) {
            if (name.startsWith(PREFIX)) merged.setProperty(name, System.getProperty(name));
        }
        ServerConfig c = new ServerConfig(merged);
        c.validate();
        return c;
    }

    /** The database password (kept out of {@link #toString()}). */
    public String dbPassword() { return dbPassword; }

    /** Throws {@link ConfigException} on the first inconsistent value; the message names the key. */
    public void validate() {
        range("tcp.port", tcpPort, 0, 65535);     // 0 = any free port (tests, embedded use)
        range("http.port", httpPort, 0, 65535);
        if (httpPort == tcpPort && tcpPort != 0) throw new ConfigException(PREFIX + "http.port must differ from " + PREFIX + "tcp.port");
        positive("io.threads", ioThreads);
        nonNegative("tcp.send.buffer", tcpSendBuffer);
        nonNegative("db.connect.retries", dbConnectRetries);
        positive("load.fetch.size", loadFetchSize);
        positive("load.parallelism", loadParallelism);
        nonNegative("load.retries", loadRetries);
        for (Map.Entry<EntityKind, Long> e : tableMinRows.entrySet()) {
            if (e.getValue() < 0) throw new ConfigException(PREFIX + "table.min.rows." + e.getKey().tableName().toLowerCase(Locale.ROOT) + " must be >= 0");
        }
        if (indexDenseThreshold < 1.0) throw new ConfigException(PREFIX + "index.dense.threshold must be >= 1.0");
        range("chunk.target.bytes", chunkTargetBytes, 64 << 10, 960 << 10);
        positive("frame.max.bytes", frameMaxBytes);
        if ((long) chunkTargetBytes + Framing.CHUNK_HEADROOM_BYTES > frameMaxBytes) {
            throw new ConfigException(PREFIX + "frame.max.bytes (" + frameMaxBytes + ") must be >= " + PREFIX + "chunk.target.bytes ("
                    + chunkTargetBytes + ") + " + Framing.CHUNK_HEADROOM_BYTES);
        }
        if (frameMaxBytes > Framing.DEFAULT_MAX_FRAME_BYTES) {
            throw new ConfigException(PREFIX + "frame.max.bytes (" + frameMaxBytes + ") must be <= " + Framing.DEFAULT_MAX_FRAME_BYTES
                    + ": every client accepts at least that, a larger frame would reject clients");
        }
        positive("max.connections", maxConnections);
        positive("max.concurrent.snapshots", maxConcurrentSnapshots);
        nonNegative("snapshot.queue.size", snapshotQueueSize);
        positiveDuration("snapshot.queue.max.wait", snapshotQueueMaxWait);
        positive("write.buffer.high.water", writeBufferHighWater);
        positive("write.buffer.low.water", writeBufferLowWater);
        if (writeBufferLowWater > writeBufferHighWater) throw new ConfigException(PREFIX + "write.buffer.low.water must be <= " + PREFIX + "write.buffer.high.water");
        if (writeBufferHighWater < frameMaxBytes) throw new ConfigException(PREFIX + "write.buffer.high.water must be >= " + PREFIX + "frame.max.bytes");
        positiveDuration("handshake.timeout", handshakeTimeout);
        positiveDuration("session.idle.timeout", sessionIdleTimeout);
        positiveDuration("stall.timeout", stallTimeout);
        positiveDuration("snapshot.max.duration", snapshotMaxDuration);
        positiveDuration("heartbeat.interval", heartbeatInterval);
        positiveDuration("shutdown.drain.timeout", shutdownDrainTimeout);
        range("live.history.batches", liveHistoryBatches, 1, MAX_LIVE_HISTORY_BATCHES);
        if (heartbeatInterval.multipliedBy(Framing.HEARTBEAT_MISSES).compareTo(snapshotQueueMaxWait) > 0) {
            throw new ConfigException(PREFIX + "heartbeat.interval x " + Framing.HEARTBEAT_MISSES + " must not exceed " + PREFIX + "snapshot.queue.max.wait");
        }
    }

    /** Every effective setting, sorted by key, with the password masked. */
    @Override public String toString() {
        Map<String, String> all = new TreeMap<>();
        all.put(PREFIX + "tcp.port", String.valueOf(tcpPort));
        all.put(PREFIX + "tcp.bind.address", bindAddress);
        all.put(PREFIX + "tcp.send.buffer", tcpSendBuffer == 0 ? "unset" : String.valueOf(tcpSendBuffer));
        all.put(PREFIX + "io.threads", String.valueOf(ioThreads));
        all.put(PREFIX + "http.port", String.valueOf(httpPort));
<<<<<<< Updated upstream
=======
        all.put(PREFIX + "admin.port", adminPort < 0 ? "disabled" : String.valueOf(adminPort));
        all.put(PREFIX + "admin.token", adminToken.isEmpty() ? "unset" : "****");
        all.put(PREFIX + "live.history.batches", String.valueOf(liveHistoryBatches));
>>>>>>> Stashed changes
        all.put(PREFIX + "inspection.enabled", String.valueOf(inspectionEnabled));
        all.put(PREFIX + "inspection.host", inspectionOptions.host());
        all.put(PREFIX + "inspection.port", String.valueOf(inspectionOptions.port()));
        all.put(PREFIX + "inspection.max.page.size", String.valueOf(inspectionOptions.maxPageSize()));
        all.put(PREFIX + "db.url", dbUrl);
        all.put(PREFIX + "db.user", dbUser);
        all.put(PREFIX + "db.password", dbPassword.isEmpty() ? "unset" : "****");
        all.put(PREFIX + "db.connect.retries", String.valueOf(dbConnectRetries));
        all.put(PREFIX + "load.fetch.size", String.valueOf(loadFetchSize));
        all.put(PREFIX + "load.parallelism", String.valueOf(loadParallelism));
        all.put(PREFIX + "load.retries", String.valueOf(loadRetries));
        all.put(PREFIX + "validation.policy", validationPolicy.name());
        for (Map.Entry<EntityKind, Long> e : tableMinRows.entrySet()) all.put(PREFIX + "table.min.rows." + e.getKey().tableName().toLowerCase(Locale.ROOT), String.valueOf(e.getValue()));
        all.put(PREFIX + "index.dense.threshold", String.valueOf(indexDenseThreshold));
        all.put(PREFIX + "chunk.target.bytes", String.valueOf(chunkTargetBytes));
        all.put(PREFIX + "frame.max.bytes", String.valueOf(frameMaxBytes));
        all.put(PREFIX + "codecs", codecs.isEmpty() ? "NONE" : codecs.toString());
        all.put(PREFIX + "max.connections", String.valueOf(maxConnections));
        all.put(PREFIX + "max.concurrent.snapshots", String.valueOf(maxConcurrentSnapshots));
        all.put(PREFIX + "snapshot.queue.size", String.valueOf(snapshotQueueSize));
        all.put(PREFIX + "snapshot.queue.max.wait", snapshotQueueMaxWait.toMillis() + "ms");
        all.put(PREFIX + "write.buffer.high.water", String.valueOf(writeBufferHighWater));
        all.put(PREFIX + "write.buffer.low.water", String.valueOf(writeBufferLowWater));
        all.put(PREFIX + "handshake.timeout", handshakeTimeout.toMillis() + "ms");
        all.put(PREFIX + "session.idle.timeout", sessionIdleTimeout.toMillis() + "ms");
        all.put(PREFIX + "stall.timeout", stallTimeout.toMillis() + "ms");
        all.put(PREFIX + "snapshot.max.duration", snapshotMaxDuration.toMillis() + "ms");
        all.put(PREFIX + "heartbeat.interval", heartbeatInterval.toMillis() + "ms");
        all.put(PREFIX + "shutdown.drain.timeout", shutdownDrainTimeout.toMillis() + "ms");
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> e : all.entrySet()) sb.append(e.getKey()).append('=').append(e.getValue()).append('\n');
        return sb.toString();
    }

    // ------------------------------------------------------------------ parsing helpers

    private String raw(String key) {
        String v = props.getProperty(PREFIX + key);
        return v == null || v.isBlank() ? null : v.trim();
    }

    private String string(String key, String dflt) {
        String v = raw(key);
        return v == null ? dflt : v;
    }

    private int intValue(String key, int dflt) {
        String v = raw(key);
        if (v == null) return dflt;
        try { return Integer.parseInt(v); }
        catch (NumberFormatException e) { throw new ConfigException(PREFIX + key + " must be an integer, got '" + v + "'"); }
    }

    private long longValue(String key, long dflt) {
        String v = raw(key);
        if (v == null) return dflt;
        try { return Long.parseLong(v); }
        catch (NumberFormatException e) { throw new ConfigException(PREFIX + key + " must be an integer, got '" + v + "'"); }
    }

    private double doubleValue(String key, double dflt) {
        String v = raw(key);
        if (v == null) return dflt;
        try { return Double.parseDouble(v); }
        catch (NumberFormatException e) { throw new ConfigException(PREFIX + key + " must be a number, got '" + v + "'"); }
    }

    private <E extends Enum<E>> E enumValue(String key, Class<E> type, E dflt) {
        String v = raw(key);
        if (v == null) return dflt;
        try { return Enum.valueOf(type, v.toUpperCase(Locale.ROOT)); }
        catch (IllegalArgumentException e) { throw new ConfigException(PREFIX + key + " must be one of " + java.util.Arrays.toString(type.getEnumConstants()) + ", got '" + v + "'"); }
    }

    private Set<Codec> codecs(String key) {
        String v = raw(key);
        Set<Codec> out = new LinkedHashSet<>();
        if (v == null) return Collections.unmodifiableSet(out);
        for (String part : v.split(",")) {
            String name = part.trim().toUpperCase(Locale.ROOT);
            if (name.isEmpty() || name.equals("NONE")) continue;
            Codec c = switch (name) {
                case "LZ4" -> Codec.CODEC_LZ4;
                case "ZSTD" -> Codec.CODEC_ZSTD;
                default -> throw new ConfigException(PREFIX + key + " must be a comma-separated list of NONE, LZ4, ZSTD, got '" + v + "'");
            };
            out.add(c);
        }
        return Collections.unmodifiableSet(out);
    }

    /** Parses a duration: a bare number is milliseconds; suffixes ms, s, m/min, h. */
    static Duration parseDuration(String key, String v) {
        String s = v.trim().toLowerCase(Locale.ROOT);
        int i = 0;
        while (i < s.length() && (Character.isDigit(s.charAt(i)) || s.charAt(i) == '.')) i++;
        String num = s.substring(0, i), unit = s.substring(i).trim();
        if (num.isEmpty()) throw new ConfigException(PREFIX + key + " must be a duration such as 30s or 10min, got '" + v + "'");
        double n = Double.parseDouble(num);
        double ms = switch (unit) {
            case "", "ms" -> n;
            case "s", "sec" -> n * 1_000;
            case "m", "min" -> n * 60_000;
            case "h" -> n * 3_600_000;
            default -> throw new ConfigException(PREFIX + key + " has an unknown duration unit '" + unit + "' (use ms, s, min, h)");
        };
        return Duration.ofMillis((long) ms);
    }

    /** Parses a size: a bare number is bytes; suffixes k/kib/kb, m/mib/mb, g/gib/gb (binary multiples). */
    static long parseSize(String key, String v) {
        String s = v.trim().toLowerCase(Locale.ROOT);
        int i = 0;
        while (i < s.length() && Character.isDigit(s.charAt(i))) i++;
        String num = s.substring(0, i), unit = s.substring(i).trim();
        if (num.isEmpty()) throw new ConfigException(PREFIX + key + " must be a size such as 262144 or 256k, got '" + v + "'");
        long n = Long.parseLong(num);
        long mult = switch (unit) {
            case "", "b" -> 1L;
            case "k", "kb", "kib" -> 1L << 10;
            case "m", "mb", "mib" -> 1L << 20;
            case "g", "gb", "gib" -> 1L << 30;
            default -> throw new ConfigException(PREFIX + key + " has an unknown size unit '" + unit + "' (use k, m, g)");
        };
        long out = n * mult;
        if (out > Integer.MAX_VALUE) throw new ConfigException(PREFIX + key + " is too large: " + v);
        return out;
    }

    private Duration duration(String key, Duration dflt) {
        String v = raw(key);
        return v == null ? dflt : parseDuration(key, v);
    }

    private long size(String key, long dflt) {
        String v = raw(key);
        return v == null ? dflt : parseSize(key, v);
    }

    private static void positive(String key, long v) {
        if (v <= 0) throw new ConfigException(PREFIX + key + " must be > 0, got " + v);
    }

    private static void nonNegative(String key, long v) {
        if (v < 0) throw new ConfigException(PREFIX + key + " must be >= 0, got " + v);
    }

    private static void range(String key, long v, long min, long max) {
        if (v < min || v > max) throw new ConfigException(PREFIX + key + " must be between " + min + " and " + max + ", got " + v);
    }

    private static void positiveDuration(String key, Duration d) {
        if (d.isZero() || d.isNegative()) throw new ConfigException(PREFIX + key + " must be a positive duration");
    }
}
