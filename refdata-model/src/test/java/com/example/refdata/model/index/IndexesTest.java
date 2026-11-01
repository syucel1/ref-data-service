package com.example.refdata.model.index;

import org.junit.Test;

import java.util.Arrays;
import java.util.SplittableRandom;

import static org.junit.Assert.*;

public class IndexesTest {

    @Test public void longIndexDenseAndSparse() {
        long[] dense = {5, 6, 7, 8, 9, 10};
        LongIndex d = LongIndex.build(dense, dense.length, 1.5);
        assertTrue(d.isDense());
        for (int i = 0; i < dense.length; i++) assertEquals(i, d.get(dense[i]));
        assertEquals(LongIndex.NOT_FOUND, d.get(4));
        assertEquals(LongIndex.NOT_FOUND, d.get(11));
        assertEquals(LongIndex.NOT_FOUND, d.get(Long.MIN_VALUE));

        long[] sparse = {1, 1000, 1_000_000, 10_000_000_000L, Long.MAX_VALUE};
        LongIndex s = LongIndex.build(sparse, sparse.length, 1.5);
        assertFalse(s.isDense());
        for (int i = 0; i < sparse.length; i++) assertEquals(i, s.get(sparse[i]));
        assertEquals(LongIndex.NOT_FOUND, s.get(2));
        assertEquals(LongIndex.NOT_FOUND, s.get(0));
    }

    @Test public void longIndexEmptyAndZeroKey() {
        LongIndex e = LongIndex.build(new long[0], 0, 1.5);
        assertEquals(LongIndex.NOT_FOUND, e.get(0));
        long[] k = {0, 3, 1_000_000_000L};
        LongIndex s = LongIndex.build(k, 3, 1.5);
        assertEquals(0, s.get(0));
        assertEquals(2, s.get(1_000_000_000L));
    }

    @Test public void longIndexRandomLarge() {
        SplittableRandom r = new SplittableRandom(42);
        long[] keys = r.longs(200_000).distinct().sorted().toArray();
        LongIndex idx = LongIndex.build(keys, keys.length, 1.5);
        for (int i = 0; i < keys.length; i += 7) assertEquals(i, idx.get(keys[i]));
        for (int i = 0; i < 1000; i++) { long probe = r.nextLong(); assertEquals(Arrays.binarySearch(keys, probe) >= 0, idx.get(probe) >= 0); }
    }

    @Test public void asciiIndexLookupsByAnyCharSequence() {
        String[] keys = {"GB00ACC100", null, "GB00ACC102", "X"};
        AsciiIndex idx = AsciiIndex.build(keys, keys.length);
        assertEquals(3, idx.size());
        assertEquals(0, idx.get("GB00ACC100"));
        assertEquals(2, idx.get(new StringBuilder("GB00ACC102")));
        assertEquals(2, idx.get(java.nio.CharBuffer.wrap("GB00ACC102")));
        assertEquals(3, idx.get("X"));
        assertEquals(AsciiIndex.NOT_FOUND, idx.get("GB00ACC101"));
        assertEquals(AsciiIndex.NOT_FOUND, idx.get(""));
        assertEquals(AsciiIndex.NOT_FOUND, idx.get("GB00ACC1000"));
    }

    @Test(expected = IllegalArgumentException.class) public void asciiIndexRejectsDuplicates() {
        AsciiIndex.build(new String[]{"A", "A"}, 2);
    }

    @Test public void asciiIndexManyKeys() {
        int n = 100_000;
        String[] keys = new String[n];
        for (int i = 0; i < n; i++) keys[i] = "K" + Integer.toString(i * 7919, 36);
        AsciiIndex idx = AsciiIndex.build(keys, n);
        for (int i = 0; i < n; i += 13) assertEquals(i, idx.get(keys[i]));
    }

    @Test public void csrIndex() {
        int[] parentOfChild = {1, 0, 1, -1, 2, 1};
        int[] order = {5, 4, 3, 2, 1, 0};          // reverse: children appear in reverse position order
        CsrIndex csr = CsrIndex.build(parentOfChild, 6, 3, order);
        assertEquals(1, csr.count(0)); assertEquals(1, csr.child(0, 0));
        assertEquals(3, csr.count(1)); assertEquals(5, csr.child(1, 0)); assertEquals(2, csr.child(1, 1)); assertEquals(0, csr.child(1, 2));
        assertEquals(1, csr.count(2)); assertEquals(4, csr.child(2, 0));
        assertEquals(0, csr.count(-1));
        assertEquals(5, csr.totalChildren());     // the orphan (-1) is not indexed
        CsrIndex natural = CsrIndex.build(parentOfChild, 6, 3, null);
        assertEquals(0, natural.child(1, 0));
    }
}
