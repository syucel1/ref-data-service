package com.example.refdata.db;

import com.example.refdata.model.EntityKind;

import java.nio.file.Path;
import java.sql.Connection;
import java.util.Locale;

/**
 * Command line: generates a database.
 * <pre>
 *   DataGenMain --db &lt;dir&gt; --profile &lt;TINY|MEDIUM|LARGE|XLARGE&gt; [--clients N] [--seed S]
 * </pre>
 * {@code --clients} overrides the profile's client count; the default seed is 1. Any existing tables in the
 * database are dropped first. Prints the row counts and the elapsed time.
 */
public final class DataGenMain {
    private DataGenMain() {
    }

    public static void main(String[] args) throws Exception {
        Path dir = null;
        DataGenerator.Profile profile = null;
        long clients = -1;
        long seed = 1;
        try {
            for (int i = 0; i < args.length; i += 2) {
                if (i + 1 >= args.length) throw new IllegalArgumentException("missing value for " + args[i]);
                String value = args[i + 1];
                switch (args[i]) {
                    case "--db" -> dir = Path.of(value);
                    case "--profile" -> profile = DataGenerator.Profile.valueOf(value.toUpperCase(Locale.ROOT));
                    case "--clients" -> clients = Long.parseLong(value);
                    case "--seed" -> seed = Long.parseLong(value);
                    default -> throw new IllegalArgumentException("unknown option " + args[i]);
                }
            }
            if (dir == null) throw new IllegalArgumentException("--db is required");
            if (profile == null && clients < 0) throw new IllegalArgumentException("--profile or --clients is required");
        } catch (IllegalArgumentException e) {
            System.err.println("error: " + e.getMessage());
            System.err.println("usage: DataGenMain --db <dir> --profile <TINY|MEDIUM|LARGE|XLARGE> [--clients N] [--seed S]");
            System.exit(2);
            return;
        }
        long clientCount = clients >= 0 ? clients : profile.clientCount();
        System.out.printf(Locale.ROOT, "Generating %,d clients (profile %s, seed %d) into %s%n", clientCount, profile == null ? "custom" : profile, seed, dir.toAbsolutePath());

        DataGenerator.Result result;
        try (EmbeddedDerby db = EmbeddedDerby.onDisk(dir); Connection c = db.connect()) {
            result = new DataGenerator(clientCount, seed).generate(c);
        }
        for (EntityKind kind : EntityKind.values()) {
            System.out.printf(Locale.ROOT, "  %-24s %,12d%n", kind.tableName(), result.rowCounts().get(kind));
        }
        long total = result.rowCounts().values().stream().mapToLong(Long::longValue).sum();
        System.out.printf(Locale.ROOT, "  %-24s %,12d%n", "total", total);
        System.out.printf(Locale.ROOT, "Elapsed %.1f s (%,d rows/s)%n", result.elapsed().toMillis() / 1000.0, total * 1000 / Math.max(1, result.elapsed().toMillis()));
    }
}
