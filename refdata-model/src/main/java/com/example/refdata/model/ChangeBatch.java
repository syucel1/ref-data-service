package com.example.refdata.model;

import java.util.*;

/** A bounded transaction applied to an immutable store as one validated publication. */
public record ChangeBatch(long baseVersion, long version, List<Change> changes) {
    public enum Operation { INSERT, UPDATE, DELETE }
    public record Change(EntityKind kind, Operation operation, String key, Object record) {
        public Change {
            Objects.requireNonNull(kind); Objects.requireNonNull(operation); Objects.requireNonNull(key);
            if (key.isBlank()) throw new IllegalArgumentException("empty primary key");
            if (operation == Operation.DELETE) {
                if (record != null) throw new IllegalArgumentException("delete must not carry a record");
            } else if (!key.equals(keyOf(kind, Objects.requireNonNull(record)))) {
                throw new IllegalArgumentException("record primary key differs from change key");
            }
        }
    }
    public ChangeBatch {
        if (baseVersion < 0 || baseVersion == Long.MAX_VALUE || version != baseVersion + 1)
            throw new IllegalArgumentException("version must immediately follow base version");
        changes = List.copyOf(changes);
        if (changes.isEmpty() || changes.size() > 1000) throw new IllegalArgumentException("batch must contain 1..1000 changes");
        Set<String> keys = new HashSet<>();
        for (Change c : changes) if (!keys.add(c.kind() + ":" + c.key()))
            throw new IllegalArgumentException("multiple mutations of one key must be coalesced before publication");
    }
    public RefDataStore apply(RefDataStore previous) {
        if (previous.dataVersion() != baseVersion) throw new IllegalArgumentException("publication version gap");
        Map<EntityKind, Map<String, Object>> rows = new EnumMap<>(EntityKind.class);
        for (EntityKind kind : EntityKind.values()) {
            Map<String, Object> values = new LinkedHashMap<>();
            for (Object row : rows(previous, kind)) values.put(keyOf(kind, row), row);
            rows.put(kind, values);
        }
        for (Change c : changes) {
            Map<String, Object> values = rows.get(c.kind());
            boolean exists = values.containsKey(c.key());
            if (c.operation() == Operation.INSERT ? exists : !exists)
                throw new IllegalArgumentException("invalid " + c.operation() + " for " + c.kind() + ":" + c.key());
            if (c.operation() == Operation.DELETE) values.remove(c.key()); else values.put(c.key(), c.record());
        }
        RefDataStoreBuilder builder = new RefDataStoreBuilder().dataVersion(version);
        rows.forEach((kind, values) -> values.values().forEach(row -> add(builder, kind, row)));
        return builder.build().store();
    }
    private static List<?> rows(RefDataStore s, EntityKind k) {
        return switch(k) {
            case COUNTRY -> s.countries(); case CURRENCY -> s.currencies(); case CLIENT -> s.clients();
            case ADDRESS -> s.addresses(); case ACCOUNT -> s.accounts(); case SETTLEMENT_INSTRUCTION -> s.settlementInstructions();
        };
    }
    private static String keyOf(EntityKind k, Object row) {
        try {
            return switch(k) {
                case COUNTRY -> ((Country) row).code(); case CURRENCY -> ((Currency) row).code();
                case CLIENT -> Long.toString(((Client) row).id()); case ADDRESS -> Long.toString(((Address) row).id());
                case ACCOUNT -> Long.toString(((Account) row).id());
                case SETTLEMENT_INSTRUCTION -> Long.toString(((SettlementInstruction) row).id());
            };
        } catch (ClassCastException e) { throw new IllegalArgumentException("record type differs from entity kind", e); }
    }
    private static void add(RefDataStoreBuilder b, EntityKind k, Object row) {
        switch(k) {
            case COUNTRY -> b.country((Country) row); case CURRENCY -> b.currency((Currency) row);
            case CLIENT -> b.client((Client) row); case ADDRESS -> b.address((Address) row);
            case ACCOUNT -> b.account((Account) row); case SETTLEMENT_INSTRUCTION -> b.settlementInstruction((SettlementInstruction) row);
        }
    }
}
