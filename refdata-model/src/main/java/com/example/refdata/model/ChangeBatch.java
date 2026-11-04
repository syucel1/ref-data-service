package com.example.refdata.model;

import java.util.EnumMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * One committed transaction of reference data changes, published as a single new data version.
 * <p>A batch moves the data from {@code baseVersion} to {@code version = baseVersion + 1}; it holds 1 to
 * {@value #MAX_CHANGES} changes, at most one per primary key (several mutations of one key must be coalesced first).
 * {@link #apply} produces the next immutable store and re-validates the complete resulting graph, so a batch that would
 * break a reference or a uniqueness rule is rejected as a whole.
 *
 * @param baseVersion data version the batch applies to
 * @param version     data version after the batch ({@code baseVersion + 1})
 * @param changes     the row changes, in the order they were written
 */
public record ChangeBatch(long baseVersion, long version, List<Change> changes) {
    /** Upper bound on the number of changes in one batch (keeps a batch well inside one frame). */
    public static final int MAX_CHANGES = 1000;

    public enum Operation { INSERT, UPDATE, DELETE }

    /**
     * One row change. {@code key} is the textual primary key ({@link EntityKind#keyOf}); {@code record} is the
     * after-image for INSERT and UPDATE and must be null for DELETE.
     */
    public record Change(EntityKind kind, Operation operation, String key, Object record) {
        public Change {
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(operation, "operation");
            Objects.requireNonNull(key, "key");
            if (key.isBlank()) throw new IllegalArgumentException("empty primary key");
            if (operation == Operation.DELETE) {
                if (record != null) throw new IllegalArgumentException("delete must not carry a record");
            } else if (!key.equals(kind.keyOf(Objects.requireNonNull(record, "record")))) {
                throw new IllegalArgumentException("record primary key differs from change key " + key);
            }
        }
    }

    public ChangeBatch {
        if (baseVersion < 0 || baseVersion == Long.MAX_VALUE || version != baseVersion + 1)
            throw new IllegalArgumentException("version must immediately follow base version");
        changes = List.copyOf(changes);
        if (changes.isEmpty() || changes.size() > MAX_CHANGES)
            throw new IllegalArgumentException("batch must contain 1.." + MAX_CHANGES + " changes");
        Set<String> keys = new HashSet<>();
        for (Change c : changes) {
            if (!keys.add(c.kind() + ":" + c.key()))
                throw new IllegalArgumentException("multiple mutations of " + c.kind() + ":" + c.key() + " must be coalesced before publication");
        }
    }

    /** Applies the batch with default build settings (validation policy FAIL). */
    public RefDataStore apply(RefDataStore previous) {
        return apply(previous, new RefDataStoreBuilder());
    }

    /**
     * Applies the batch to {@code previous} and builds the next store with the given (fresh, empty) builder, so the
     * caller's index settings are kept. The previous store is not modified.
     * @throws IllegalArgumentException when the store is not at {@code baseVersion}, or an INSERT hits an existing key
     *                                  or an UPDATE/DELETE a missing one
     * @throws com.example.refdata.model.validation.ValidationException when the resulting data breaks a rule
     */
    public RefDataStore apply(RefDataStore previous, RefDataStoreBuilder builder) {
        if (previous.dataVersion() != baseVersion)
            throw new IllegalArgumentException("publication version gap: store is at " + previous.dataVersion() + ", batch expects " + baseVersion);
        Map<EntityKind, Map<String, Object>> touched = touchedRowsByKey(previous);
        for (Change c : changes) {
            Map<String, Object> rows = touched.get(c.kind());
            boolean exists = rows.containsKey(c.key());
            boolean valid = c.operation() == Operation.INSERT ? !exists : exists;
            if (!valid) throw new IllegalArgumentException("invalid " + c.operation() + " for " + c.kind() + ":" + c.key());
            if (c.operation() == Operation.DELETE) rows.remove(c.key());
            else rows.put(c.key(), c.record());
        }
        builder.dataVersion(version);
        for (EntityKind kind : EntityKind.values()) {
            Map<String, Object> changed = touched.get(kind);
            Iterable<?> rows = changed != null ? changed.values() : previous.rows(kind);   // untouched kinds are copied as they are
            for (Object row : rows) builder.accept(kind, row);
        }
        return builder.build().store();
    }

    /** Key -> row maps for the kinds this batch changes (only those: the others need no lookups). */
    private Map<EntityKind, Map<String, Object>> touchedRowsByKey(RefDataStore previous) {
        Map<EntityKind, Map<String, Object>> touched = new EnumMap<>(EntityKind.class);
        for (Change c : changes) {
            touched.computeIfAbsent(c.kind(), kind -> {
                List<?> rows = previous.rows(kind);
                Map<String, Object> byKey = new LinkedHashMap<>(rows.size() * 2);
                for (Object row : rows) byKey.put(kind.keyOf(row), row);
                return byKey;
            });
        }
        return touched;
    }
}
