package com.example.refdata.loader.jdbc;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class StringCacheTest {

    @Test
    public void returnsTheFirstInstanceForEqualValues() {
        StringCache cache = new StringCache();
        String first = new String("Zurich");
        assertSame(first, cache.dedupe(first));
        assertSame(first, cache.dedupe(new String("Zurich")));
        assertNull(cache.dedupe(null));
        assertEquals(1, cache.size());
    }

    @Test
    public void stopsRememberingNewValuesAtTheCap() {
        StringCache cache = new StringCache(2);
        cache.dedupe("a");
        cache.dedupe("b");
        String c = new String("c");
        assertSame(c, cache.dedupe(c));
        assertNotSame(c, cache.dedupe(new String("c")));      // not cached: passes through
        assertSame("a", cache.dedupe(new String("a")));       // earlier values still served
        assertEquals(2, cache.size());
    }
}
