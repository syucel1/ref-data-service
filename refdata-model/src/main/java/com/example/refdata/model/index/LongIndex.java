package com.example.refdata.model.index;

import java.util.Arrays;

/**
 * Maps a {@code long} key to an {@code int} position without boxing. Dense keys use a direct array; otherwise
 * open addressing over parallel {@code long[]}/{@code int[]} arrays (load factor 0.5, never resized after build).
 * Immutable after construction; safe for concurrent readers.
 */
public final class LongIndex {
    public static final int NOT_FOUND = -1;
    private static final int EMPTY = -1;

    private final boolean dense;
    private final long base;
    private final int[] denseSlots;      // dense mode: position by (key - base)
    private final long[] keys;           // hash mode
    private final int[] positions;
    private final int mask;
    private final int size;

    /** Builds an index over sorted, distinct keys where key[i] maps to position i. */
    public static LongIndex build(long[] sortedKeys, int n, double denseThreshold) {
        if (n == 0) return new LongIndex(true, 0, new int[0], null, null, 0, 0);
        long min = sortedKeys[0], max = sortedKeys[n - 1];
        long span = max - min + 1;
        if (span > 0 && span <= (long) (n * denseThreshold) && span < Integer.MAX_VALUE - 8) {
            int[] slots = new int[(int) span];
            Arrays.fill(slots, EMPTY);
            for (int i = 0; i < n; i++) slots[(int) (sortedKeys[i] - min)] = i;
            return new LongIndex(true, min, slots, null, null, 0, n);
        }
        int cap = Integer.highestOneBit(Math.max(4, n * 2 - 1)) << 1;   // >= 2n, power of two
        long[] ks = new long[cap];
        int[] ps = new int[cap];
        Arrays.fill(ps, EMPTY);
        int m = cap - 1;
        for (int i = 0; i < n; i++) {
            int h = mix(sortedKeys[i]) & m;
            while (ps[h] != EMPTY) h = (h + 1) & m;
            ks[h] = sortedKeys[i];
            ps[h] = i;
        }
        return new LongIndex(false, 0, null, ks, ps, m, n);
    }

    private LongIndex(boolean dense, long base, int[] denseSlots, long[] keys, int[] positions, int mask, int size) {
        this.dense = dense; this.base = base; this.denseSlots = denseSlots; this.keys = keys;
        this.positions = positions; this.mask = mask; this.size = size;
    }

    /** Position of the key, or {@link #NOT_FOUND}. Allocation-free. */
    public int get(long key) {
        if (dense) {
            long off = key - base;
            return off >= 0 && off < denseSlots.length ? denseSlots[(int) off] : NOT_FOUND;
        }
        int h = mix(key) & mask;
        while (true) {
            int p = positions[h];
            if (p == EMPTY) return NOT_FOUND;
            if (keys[h] == key) return p;
            h = (h + 1) & mask;
        }
    }

    public int size() { return size; }
    public boolean isDense() { return dense; }

    private static int mix(long k) {
        k ^= k >>> 33; k *= 0xff51afd7ed558ccdL; k ^= k >>> 33; k *= 0xc4ceb9fe1a85ec53L; k ^= k >>> 33;
        return (int) k;
    }
}
