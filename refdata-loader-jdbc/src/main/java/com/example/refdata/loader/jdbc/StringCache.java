package com.example.refdata.loader.jdbc;

import java.util.HashMap;
import java.util.Map;

/**
 * De-duplicates repeated string values while rows stream (design 8.1: one instance per distinct country code, city,
 * BIC and so on, instead of one per row). Not thread-safe: each table reader owns one. Capped so a column that turns
 * out to have many distinct values cannot grow the cache without bound; past the cap values pass through unchanged.
 */
final class StringCache {
    static final int DEFAULT_CAPACITY = 100_000;

    private final Map<String, String> values;
    private final int capacity;

    StringCache() {
        this(DEFAULT_CAPACITY);
    }

    StringCache(int capacity) {
        this.capacity = capacity;
        this.values = new HashMap<>(Math.min(capacity, 4_096) * 2);
    }

    /** The canonical instance equal to {@code s} (or {@code s} itself when null, first seen, or the cache is full). */
    String dedupe(String s) {
        if (s == null) return null;
        String existing = values.get(s);
        if (existing != null) return existing;
        if (values.size() < capacity) values.put(s, s);
        return s;
    }

    int size() {
        return values.size();
    }
}
