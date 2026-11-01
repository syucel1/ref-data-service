package com.example.refdata.model;

import org.junit.Test;

import static com.example.refdata.model.Fixtures.*;
import static org.junit.Assert.*;

public class RefDataStoreTest {
    private final RefDataStore store = validSet().build().store();

    @Test public void countsAndVersion() {
        assertEquals(2, store.count(EntityKind.COUNTRY));
        assertEquals(3, store.count(EntityKind.CURRENCY));
        assertEquals(4, store.count(EntityKind.CLIENT));
        assertEquals(5, store.count(EntityKind.ADDRESS));
        assertEquals(5, store.count(EntityKind.ACCOUNT));
        assertEquals(6, store.count(EntityKind.SETTLEMENT_INSTRUCTION));
        assertEquals(25, store.totalRows());
        assertEquals(7, store.dataVersion());
    }

    @Test public void rowsAreInPrimaryKeyOrder() {
        assertEquals(1, store.clients().get(0).id());
        assertEquals(10, store.clients().get(3).id());
        assertEquals("DE", store.countries().get(0).code());
        assertEquals(1000, store.settlementInstructions().get(0).id());
    }

    @Test public void lookupTables() {
        assertEquals("United Kingdom", store.country("GB").name());
        assertEquals("GB", store.countryByIso3("GBR").code());
        assertEquals("DE", store.countryByNumeric(276).code());
        assertNull(store.country("XX"));
        assertNull(store.countryByNumeric(999));
        assertNull(store.countryByNumeric(-1));
        assertEquals(0, store.currency("JPY").minorUnits());
        assertEquals("EUR", store.currencyByNumeric(978).code());
        assertEquals("GB", store.country(new StringBuilder("GB")).code());   // any CharSequence
    }

    @Test public void clientsAndHierarchy() {
        assertEquals("C2", store.client(2).ref());
        assertNull(store.client(4));
        assertEquals(3, store.clientByRef("C3").id());
        assertEquals(1, store.clientByLei("LEI00000000000000001").id());
        assertNull(store.clientByLei("nope"));
        assertNull(store.parentOf(store.client(1)));
        assertEquals(1, store.parentOf(store.client(2)).id());
        assertEquals(1, store.ultimateParentOf(store.client(3)).id());
        assertEquals(10, store.ultimateParentOf(store.client(10)).id());
        assertEquals(1, store.childCount(1));
        assertEquals(2, store.child(1, 0).id());
        assertEquals(0, store.childCount(10));
        assertEquals(0, store.childCount(999));            // unknown client: no children, no exception
    }

    @Test public void addresses() {
        assertEquals(3, store.addressCountOfClient(1));
        assertEquals(AddressType.REGISTERED, store.addressOfClient(1, 0).type());   // ordered by type, then validFrom
        assertEquals(2, store.addressOfClient(1, 1).id());
        assertEquals(3, store.addressOfClient(1, 2).id());
        assertEquals(2, store.primaryAddress(1, AddressType.MAILING, D2020 + 10).id());
        assertEquals(3, store.primaryAddress(1, AddressType.MAILING, D2022).id());      // validFrom inclusive
        assertEquals(2, store.primaryAddress(1, AddressType.MAILING, D2022 - 1).id());  // validTo exclusive
        assertNull(store.primaryAddress(1, AddressType.BILLING, D2024));
        assertNull(store.primaryAddress(10, AddressType.BILLING, D2024));              // exists but not primary
        assertNull(store.address(77));
        assertEquals("Floor 2", store.address(2).line2());
        assertNull(store.address(1).line2());
    }

    @Test public void accounts() {
        assertEquals(2, store.accountCountOfClient(1));
        assertEquals(100, store.accountOfClient(1, 0).id());
        assertEquals(101, store.accountByNumber("GB00ACC101").id());
        assertTrue(store.account(101).isClosed());
        assertFalse(store.account(100).isClosed());
        assertNull(store.accountByNumber("GB00ACC999"));
    }

    @Test public void settlementInstructions() {
        assertEquals(4, store.ssiCountOfAccount(100));
        assertEquals("EUR", store.ssiOfAccount(100, 0).currencyCode());                 // ordered by currency, asset class, from
        assertEquals(1000, store.defaultSsi(100, "GBP", AssetClass.CASH, D2020 + 5).id());
        assertEquals(1001, store.defaultSsi(100, "GBP", AssetClass.CASH, D2024).id());
        assertEquals(1002, store.defaultSsi(100, "EUR", AssetClass.CASH, D2024).id());
        assertNull(store.defaultSsi(100, "GBP", AssetClass.EQUITY, D2024));             // exists but not default
        assertNull(store.defaultSsi(100, "JPY", AssetClass.CASH, D2024));
        assertNull(store.defaultSsi(999, "GBP", AssetClass.CASH, D2024));
        assertEquals(1005, store.settlementInstruction(1005).id());
    }

    @Test public void enumsMapUnknownSafely() {
        assertEquals(ClientType.UNKNOWN, ClientType.fromWire(99));
        assertEquals(ClientType.UNKNOWN, ClientType.fromWire(0));
        assertEquals(ClientType.BANK, ClientType.fromWire(ClientType.BANK.wireCode()));
        assertEquals(Status.CLOSED, Status.fromDb("CLOSED"));
        assertNull(Status.fromDb("WHATEVER"));
        assertNull(Status.fromDb(null));
        assertEquals(AssetClass.FIXED_INCOME, AssetClass.fromDb("FIXED_INCOME"));
        assertEquals(EntityKind.ACCOUNT, EntityKind.fromWire(5));
        assertNull(EntityKind.fromWire(42));
    }
}
