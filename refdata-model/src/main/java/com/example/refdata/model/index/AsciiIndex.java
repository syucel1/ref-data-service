package com.example.refdata.model.index;

import java.util.Arrays;

/**
 * Maps a short ASCII/Latin-1 string key (client reference, LEI, account number …) to an {@code int} position.
 * Lookups accept any {@link CharSequence} and compare character by character, so a caller can look up straight
 * from a reusable buffer without creating a {@code String}. Open addressing, load factor 0.5, immutable after build.
 */
public final class AsciiIndex {
    public static final int NOT_FOUND = -1;
    private final String[] keys;
    private final int[] positions;
    private final int mask;
    private final int size;

    /** Builds from parallel arrays; a null key is skipped (nullable unique columns such as LEI). Duplicate keys must have been rejected before. */
    public static AsciiIndex build(String[] keyByPosition, int n) {
        int live = 0;
        for (int i = 0; i < n; i++) if (keyByPosition[i] != null) live++;
        int cap = Integer.highestOneBit(Math.max(4, live * 2 - 1)) << 1;
        String[] ks = new String[cap];
        int[] ps = new int[cap];
        Arrays.fill(ps, NOT_FOUND);
        int m = cap - 1;
        for (int i = 0; i < n; i++) {
            String k = keyByPosition[i];
            if (k == null) continue;
            int h = hash(k) & m;
            while (ps[h] != NOT_FOUND) {
                if (ks[h].contentEquals(k)) throw new IllegalArgumentException("duplicate key " + k);
                h = (h + 1) & m;
            }
            ks[h] = k;
            ps[h] = i;
        }
        return new AsciiIndex(ks, ps, m, live);
    }

    private AsciiIndex(String[] keys, int[] positions, int mask, int size) {
        this.keys = keys; this.positions = positions; this.mask = mask; this.size = size;
    }

    /** Position for the key, or {@link #NOT_FOUND}. Allocation-free. */
    public int get(CharSequence key) {
        int h = hash(key) & mask;
        while (true) {
            int p = positions[h];
            if (p == NOT_FOUND) return NOT_FOUND;
            if (equals(keys[h], key)) return p;
            h = (h + 1) & mask;
        }
    }

    public int size() { return size; }

    static int hash(CharSequence s) {
        int h = 0x9747b28c;
        for (int i = 0, n = s.length(); i < n; i++) h = (h ^ s.charAt(i)) * 0x01000193;
        return h ^ (h >>> 16);
    }

    private static boolean equals(String a, CharSequence b) {
        int n = a.length();
        if (n != b.length()) return false;
        for (int i = 0; i < n; i++) if (a.charAt(i) != b.charAt(i)) return false;
        return true;
    }
}
