package com.example.refdata.model;

import com.example.refdata.model.validation.ValidationException;
import com.example.refdata.model.validation.ValidationPolicy;
import com.example.refdata.model.validation.ValidationReport;
import com.example.refdata.model.validation.Violation;
import org.junit.Test;

import static com.example.refdata.model.Fixtures.*;
import static org.junit.Assert.*;

public class RefDataStoreBuilderValidationTest {

    private static ValidationReport failsWith(RefDataStoreBuilder b, Violation.Rule rule) {
        try {
            b.build();
            fail("expected a ValidationException with " + rule);
            return null;
        } catch (ValidationException e) {
            assertTrue("expected " + rule + " in " + e.report(), e.report().countByRule().containsKey(rule));
            return e.report();
        }
    }

    @Test public void cleanDataBuilds() {
        RefDataStoreBuilder.Result r = validSet().build();
        assertTrue(r.report().isClean());
        assertEquals(25, r.store().totalRows());
    }

    @Test public void duplicateAccountNumberFails() {
        RefDataStoreBuilder b = validSet();
        b.account(account(105, "GB00ACC100", 1, "GBP", Account.NOT_CLOSED));
        ValidationReport r = failsWith(b, Violation.Rule.DUPLICATE_KEY);
        assertEquals(1, r.totalViolations());
        assertEquals(EntityKind.ACCOUNT, r.violations().get(0).kind());
    }

    @Test public void duplicatePrimaryKeyFails() {
        RefDataStoreBuilder b = validSet();
        b.client(client(1, "C1dup", null, Client.NO_PARENT));
        failsWith(b, Violation.Rule.DUPLICATE_KEY);
    }

    @Test public void orphanAccountFails() {
        RefDataStoreBuilder b = validSet();
        b.account(account(105, "GB00ACC105", 999, "GBP", Account.NOT_CLOSED));
        failsWith(b, Violation.Rule.MISSING_REFERENCE);
    }

    @Test public void unknownCountryOrCurrencyFails() {
        RefDataStoreBuilder b = validSet();
        b.account(account(105, "GB00ACC105", 1, "XXX", Account.NOT_CLOSED));
        failsWith(b, Violation.Rule.MISSING_REFERENCE);
    }

    @Test public void missingMandatoryAndUnknownEnumFail() {
        RefDataStoreBuilder b = validSet();
        b.client(new Client(20, "C20", null, "Name", null, null, Status.ACTIVE, "GB", Client.NO_PARENT, 1, D2020, 1, T0));   // type null = unknown DB code
        failsWith(b, Violation.Rule.MANDATORY_MISSING);
    }

    @Test public void badCodesFail() {
        RefDataStoreBuilder b = validSet();
        b.country(new Country("gb", "XGB", 1, "lower", "Europe"));
        failsWith(b, Violation.Rule.INVALID_VALUE);
    }

    @Test public void hierarchyCycleFails() {
        RefDataStoreBuilder b = validSet();
        b.client(client(30, "C30", null, 31));
        b.client(client(31, "C31", null, 32));
        b.client(client(32, "C32", null, 30));
        ValidationReport r = failsWith(b, Violation.Rule.HIERARCHY_CYCLE);
        assertEquals(3, r.countByRule().get(Violation.Rule.HIERARCHY_CYCLE).intValue());
    }

    @Test public void selfParentFails() {
        RefDataStoreBuilder b = validSet();
        b.client(client(30, "C30", null, 30));
        failsWith(b, Violation.Rule.HIERARCHY_CYCLE);
    }

    @Test public void overlappingDefaultSsisFail() {
        RefDataStoreBuilder b = validSet();
        b.settlementInstruction(ssi(1006, 100, "GBP", AssetClass.CASH, true, D2020 + 100, D2020 + 200));   // overlaps 1000
        failsWith(b, Violation.Rule.OVERLAPPING_PERIODS);
    }

    @Test public void nonDefaultOverlapsAreFine() {
        RefDataStoreBuilder b = validSet();
        b.settlementInstruction(ssi(1006, 100, "GBP", AssetClass.CASH, false, D2020 + 100, D2020 + 200));
        assertTrue(b.build().report().isClean());
    }

    @Test public void overlappingPrimaryAddressesFail() {
        RefDataStoreBuilder b = validSet();
        b.address(address(6, 1, AddressType.REGISTERED, true, D2022, Address.OPEN_ENDED));
        failsWith(b, Violation.Rule.OVERLAPPING_PERIODS);
    }

    @Test public void invalidDateRangesFail() {
        RefDataStoreBuilder b = validSet();
        b.address(address(6, 10, AddressType.MAILING, false, D2022, D2020));
        failsWith(b, Violation.Rule.INVALID_DATE_RANGE);
    }

    @Test public void tooFewRowsFailsEvenWhenQuarantining() {
        RefDataStoreBuilder b = validSet().policy(ValidationPolicy.QUARANTINE).minRows(EntityKind.CLIENT, 100);
        failsWith(b, Violation.Rule.TOO_FEW_ROWS);
    }

    @Test public void quarantineDropsBadRowsAndTheirDependants() {
        RefDataStoreBuilder b = validSet().policy(ValidationPolicy.QUARANTINE);
        b.client(client(40, "C40", null, 999));                                     // missing parent -> dropped
        b.account(account(400, "GB00ACC400", 40, "GBP", Account.NOT_CLOSED));       // depends on 40 -> dropped
        b.settlementInstruction(ssi(4000, 400, "GBP", AssetClass.CASH, true, D2020, SettlementInstruction.OPEN_ENDED)); // depends on 400
        b.address(address(40, 40, AddressType.REGISTERED, true, D2020, Address.OPEN_ENDED));
        RefDataStoreBuilder.Result r = b.build();
        assertFalse(r.report().isClean());
        assertEquals(1, r.report().quarantinedByKind().get(EntityKind.CLIENT).intValue());
        assertEquals(1, r.report().quarantinedByKind().get(EntityKind.ACCOUNT).intValue());
        assertEquals(1, r.report().quarantinedByKind().get(EntityKind.SETTLEMENT_INSTRUCTION).intValue());
        assertEquals(1, r.report().quarantinedByKind().get(EntityKind.ADDRESS).intValue());
        assertEquals(25, r.store().totalRows());
        assertNull(r.store().client(40));
        assertNull(r.store().account(400));
    }

    @Test public void subsetLoadSkipsReferencesIntoKindsNotLoaded() {
        RefDataStoreBuilder b = new RefDataStoreBuilder().loadedKinds(java.util.EnumSet.of(EntityKind.COUNTRY, EntityKind.CURRENCY, EntityKind.ACCOUNT));
        b.country(gb()); b.currency(gbp());
        b.account(account(1, "N1", 12345, "GBP", Account.NOT_CLOSED));      // client 12345 not loaded -> fine
        RefDataStoreBuilder.Result r = b.build();
        assertTrue(r.report().isClean());
        assertNull(r.store().client(12345));
        assertEquals(0, r.store().accountCountOfClient(12345));
        assertEquals(1, r.store().accountByNumber("N1").id());
        // but a reference into a loaded kind is still checked
        RefDataStoreBuilder b2 = new RefDataStoreBuilder().loadedKinds(java.util.EnumSet.of(EntityKind.COUNTRY, EntityKind.CURRENCY, EntityKind.ACCOUNT));
        b2.country(gb()); b2.currency(gbp());
        b2.account(account(1, "N1", 12345, "XXX", Account.NOT_CLOSED));
        failsWith(b2, Violation.Rule.MISSING_REFERENCE);
    }

    @Test public void overlapFlagsEveryRowInsideAnEarlierLongPeriod() {
        // A [0,100) then B [10,20) and C [30,40): both B and C overlap A, even though C does not overlap B
        RefDataStoreBuilder b = validSet();
        b.settlementInstruction(ssi(1006, 104, "EUR", AssetClass.CASH, true, D2020, D2020 + 100));
        b.settlementInstruction(ssi(1007, 104, "EUR", AssetClass.CASH, true, D2020 + 10, D2020 + 20));
        b.settlementInstruction(ssi(1008, 104, "EUR", AssetClass.CASH, true, D2020 + 30, D2020 + 40));
        ValidationReport r = failsWith(b, Violation.Rule.OVERLAPPING_PERIODS);
        assertEquals(2, r.countByRule().get(Violation.Rule.OVERLAPPING_PERIODS).intValue());

        RefDataStoreBuilder q = validSet().policy(ValidationPolicy.QUARANTINE);
        q.settlementInstruction(ssi(1006, 104, "EUR", AssetClass.CASH, true, D2020, D2020 + 100));
        q.settlementInstruction(ssi(1007, 104, "EUR", AssetClass.CASH, true, D2020 + 10, D2020 + 20));
        q.settlementInstruction(ssi(1008, 104, "EUR", AssetClass.CASH, true, D2020 + 30, D2020 + 40));
        RefDataStoreBuilder.Result res = q.build();
        assertEquals(2, res.report().quarantinedByKind().get(EntityKind.SETTLEMENT_INSTRUCTION).intValue());
        assertEquals(1006, res.store().defaultSsi(104, "EUR", AssetClass.CASH, D2020 + 35).id());
    }

    @Test public void duplicatesAcrossNonAdjacentRowsAreFound() {
        RefDataStoreBuilder b = validSet();
        b.client(client(50, "C1", null, Client.NO_PARENT));          // same ref as client 1, far apart in id order
        b.client(client(60, "C60", "LEI00000000000000001", Client.NO_PARENT));
        ValidationReport r = failsWith(b, Violation.Rule.DUPLICATE_KEY);
        assertEquals(2, r.countByRule().get(Violation.Rule.DUPLICATE_KEY).intValue());
    }

    @Test public void unknownParentAccessorsReturnNull() {
        RefDataStore s = validSet().build().store();
        assertNull(s.child(999, 0));
        assertNull(s.addressOfClient(999, 0));
        assertNull(s.accountOfClient(999, 0));
        assertNull(s.ssiOfAccount(999, 0));
    }

    @Test public void differentKindsMayBeFedFromDifferentThreads() throws Exception {
        RefDataStoreBuilder b = new RefDataStoreBuilder();
        int clients = 20_000;
        Thread[] threads = {
            new Thread(() -> { b.country(gb()); b.country(de()); }),
            new Thread(() -> { b.currency(gbp()); b.currency(eur()); b.currency(jpy()); }),
            new Thread(() -> { for (long i = 1; i <= clients; i++) b.client(client(i, "R" + i, null, i > 1 && i % 10 == 0 ? i - 1 : Client.NO_PARENT)); }),
            new Thread(() -> { for (long i = 1; i <= clients; i++) b.address(address(i, i, AddressType.REGISTERED, true, D2020, Address.OPEN_ENDED)); }),
            new Thread(() -> { for (long i = 1; i <= clients * 2; i++) b.account(account(i, "N" + i, (i + 1) / 2, "GBP", Account.NOT_CLOSED)); }),
            new Thread(() -> { for (long i = 1; i <= clients * 2; i++) b.settlementInstruction(ssi(i, i, "GBP", AssetClass.CASH, true, D2020, SettlementInstruction.OPEN_ENDED)); }),
        };
        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join();
        RefDataStoreBuilder.Result r = b.build();
        assertTrue(r.report().isClean());
        assertEquals(clients, r.store().count(EntityKind.CLIENT));
        assertEquals(clients * 2, r.store().count(EntityKind.SETTLEMENT_INSTRUCTION));
        assertEquals(2, r.store().accountCountOfClient(7));
        assertEquals(1, r.store().childCount(9));
    }

    @Test public void reportCapsDetailButCountsAll() {
        RefDataStoreBuilder b = validSet();
        for (int i = 0; i < 150; i++) b.account(account(200 + i, "N" + i, 5000 + i, "GBP", Account.NOT_CLOSED));
        ValidationReport r = failsWith(b, Violation.Rule.MISSING_REFERENCE);
        assertEquals(150, r.totalViolations());
        assertEquals(ValidationReport.MAX_DETAILED, r.violations().size());
        assertTrue(r.toString().contains("and 50 more"));
    }
}
