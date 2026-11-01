package com.example.refdata.contract;

import com.example.refdata.model.*;
import org.junit.Test;

import static org.junit.Assert.*;

public class EntityCodecTest {
    private static final int D = 18000;
    private static final long T = 1_700_000_000_000L;

    @Test public void countryAndCurrencyRoundTrip() {
        Country c = new Country("GB", "GBR", 826, "United Kingdom", null);
        assertEquals(new Country("GB", "GBR", 826, "United Kingdom", null), EntityCodec.fromProto(EntityCodec.toProto(c)));
        Currency cu = new Currency("JPY", 392, "Yen", 0, false);
        assertEquals(cu, EntityCodec.fromProto(EntityCodec.toProto(cu)));
    }

    @Test public void clientRoundTripWithAndWithoutOptionals() {
        Client full = new Client(5, "C5", "LEI00000000000000005", "Legal", "Short", ClientType.FUND, Status.SUSPENDED, "DE", 2, 4, D, 3, T);
        assertEquals(full, EntityCodec.fromProto(EntityCodec.toProto(full)));
        Client bare = new Client(6, "C6", null, "Legal", null, ClientType.INDIVIDUAL, Status.ACTIVE, "GB", Client.NO_PARENT, 0, D, 1, T);
        com.example.refdata.schema.v1.Client p = EntityCodec.toProto(bare);
        assertFalse(p.hasLei());
        assertFalse(p.hasParentClientId());
        assertEquals(bare, EntityCodec.fromProto(p));
    }

    @Test public void addressAccountSsiRoundTrip() {
        Address a = new Address(1, 2, AddressType.OPERATIONAL, "L1", "L2", null, "City", null, "PC", "GB", true, D, Address.OPEN_ENDED, 1, T);
        assertEquals(a, EntityCodec.fromProto(EntityCodec.toProto(a)));
        assertFalse(EntityCodec.toProto(a).hasValidTo());
        Address dated = new Address(1, 2, AddressType.BILLING, "L1", null, "L3", "City", "R", "PC", "GB", false, D, D + 10, 1, T);
        assertEquals(dated, EntityCodec.fromProto(EntityCodec.toProto(dated)));

        Account open = new Account(9, "GB00X", 2, AccountType.MARGIN, "EUR", "DE", Status.ACTIVE, D, Account.NOT_CLOSED, 1, T);
        assertEquals(open, EntityCodec.fromProto(EntityCodec.toProto(open)));
        assertFalse(EntityCodec.toProto(open).hasClosedDate());
        Account closed = new Account(9, "GB00X", 2, AccountType.MARGIN, "EUR", "DE", Status.CLOSED, D, D + 1, 1, T);
        assertEquals(closed, EntityCodec.fromProto(EntityCodec.toProto(closed)));

        SettlementInstruction s = new SettlementInstruction(3, 9, "EUR", AssetClass.FIXED_INCOME, "BIC1", "BIC2", "ACC", true, D, SettlementInstruction.OPEN_ENDED, 1, T);
        assertEquals(s, EntityCodec.fromProto(EntityCodec.toProto(s)));
        SettlementInstruction ended = new SettlementInstruction(3, 9, "EUR", AssetClass.CASH, "BIC1", "BIC2", "ACC", false, D, D + 5, 1, T);
        assertEquals(ended, EntityCodec.fromProto(EntityCodec.toProto(ended)));
    }

    @Test public void unknownEnumNumbersFromANewerServerBecomeUnknown() throws Exception {
        com.example.refdata.schema.v1.Account p = com.example.refdata.schema.v1.Account.newBuilder().setAccountId(1).setAccountNumber("N").setClientId(1)
                .setAccountTypeValue(77).setStatusValue(42).setBaseCurrency("EUR").setBookingCountry("DE").build();
        // re-parse from bytes as a client would
        com.example.refdata.schema.v1.Account parsed = com.example.refdata.schema.v1.Account.parseFrom(p.toByteArray());
        Account a = EntityCodec.fromProto(parsed);
        assertEquals(AccountType.UNKNOWN, a.type());
        assertEquals(Status.UNKNOWN, a.status());
    }

    @Test public void wireNumbersMatchTheProtoEnums() {
        assertEquals(com.example.refdata.schema.v1.ClientType.CLIENT_TYPE_BANK_VALUE, ClientType.BANK.wireCode());
        assertEquals(com.example.refdata.schema.v1.Status.STATUS_CLOSED_VALUE, Status.CLOSED.wireCode());
        assertEquals(com.example.refdata.schema.v1.AddressType.ADDRESS_TYPE_OPERATIONAL_VALUE, AddressType.OPERATIONAL.wireCode());
        assertEquals(com.example.refdata.schema.v1.AccountType.ACCOUNT_TYPE_TRADING_VALUE, AccountType.TRADING.wireCode());
        assertEquals(com.example.refdata.schema.v1.AssetClass.ASSET_CLASS_FIXED_INCOME_VALUE, AssetClass.FIXED_INCOME.wireCode());
        for (EntityKind k : EntityKind.values())
            assertEquals(k.name(), com.example.refdata.schema.v1.EntityType.valueOf("ENTITY_TYPE_" + k.name()).getNumber(), k.wireCode());
    }

    @Test public void concatenatedBatchesEqualOneBigBatch() throws Exception {
        var a = com.example.refdata.schema.v1.CurrencyBatch.newBuilder().addItems(EntityCodec.toProto(new Currency("EUR", 978, "Euro", 2, true))).build().toByteArray();
        var b = com.example.refdata.schema.v1.CurrencyBatch.newBuilder().addItems(EntityCodec.toProto(new Currency("GBP", 826, "Pound", 2, true))).build().toByteArray();
        var both = com.example.refdata.schema.v1.CurrencyBatch.newBuilder()
                .addItems(EntityCodec.toProto(new Currency("EUR", 978, "Euro", 2, true)))
                .addItems(EntityCodec.toProto(new Currency("GBP", 826, "Pound", 2, true))).build().toByteArray();
        byte[] concat = new byte[a.length + b.length];
        System.arraycopy(a, 0, concat, 0, a.length); System.arraycopy(b, 0, concat, a.length, b.length);
        assertArrayEquals(both, concat);
    }
}
