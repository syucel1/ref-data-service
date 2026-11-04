package com.example.refdata.model;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

public class CodedEnumTest {
    @Test public void wireCodesMapByValueAndUnknownNeverThrows() {
        for (Status s : Status.values()) assertEquals(s, Status.fromWire(s.wireCode()));
        assertEquals(Status.UNKNOWN, Status.fromWire(99));
        assertEquals(Status.UNKNOWN, Status.fromWire(-1));
        assertEquals(AssetClass.FIXED_INCOME, AssetClass.fromWire(3));
    }

    @Test public void dbCodesMapAndUnknownIsNull() {
        assertEquals(AccountType.MARGIN, AccountType.fromDb("MARGIN"));
        assertNull(AccountType.fromDb("UNKNOWN"));
        assertNull(AccountType.fromDb("NOPE"));
        assertNull(AccountType.fromDb(null));
    }

    @Test public void entityKindKeys() {
        assertEquals("GB", EntityKind.COUNTRY.keyOf(new Country("GB", "GBR", 826, "UK", null)));
        assertThrows(IllegalArgumentException.class, () -> EntityKind.CURRENCY.keyOf(new Country("GB", "GBR", 826, "UK", null)));
    }
}
