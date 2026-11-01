package com.example.refdata.model;

/** Hand-made rows covering every entity kind and every optional field, for unit tests. */
public final class Fixtures {
    private Fixtures() {}

    public static final int D2020 = (int) java.time.LocalDate.of(2020, 1, 1).toEpochDay();
    public static final int D2022 = (int) java.time.LocalDate.of(2022, 1, 1).toEpochDay();
    public static final int D2024 = (int) java.time.LocalDate.of(2024, 1, 1).toEpochDay();
    public static final long T0 = 1_700_000_000_000L;

    public static Country gb() { return new Country("GB", "GBR", 826, "United Kingdom", "Europe"); }
    public static Country de() { return new Country("DE", "DEU", 276, "Germany", "Europe"); }
    public static Currency gbp() { return new Currency("GBP", 826, "Pound sterling", 2, true); }
    public static Currency eur() { return new Currency("EUR", 978, "Euro", 2, true); }
    public static Currency jpy() { return new Currency("JPY", 392, "Yen", 0, true); }

    public static Client client(long id, String ref, String lei, long parent) {
        return new Client(id, ref, lei, "Legal " + ref, "Short " + ref, ClientType.CORPORATE, Status.ACTIVE, "GB", parent, 3, D2020, 1, T0 + id);
    }
    public static Address address(long id, long clientId, AddressType type, boolean primary, int from, int to) {
        return new Address(id, clientId, type, "1 High Street", id % 2 == 0 ? "Floor 2" : null, null, "London", id % 3 == 0 ? "Greater London" : null,
                "EC1A 1BB", "GB", primary, from, to, 1, T0 + id);
    }
    public static Account account(long id, String number, long clientId, String ccy, int closed) {
        return new Account(id, number, clientId, AccountType.CASH, ccy, "GB", Status.ACTIVE, D2020, closed, 2, T0 + id);
    }
    public static SettlementInstruction ssi(long id, long accountId, String ccy, AssetClass ac, boolean dflt, int from, int to) {
        return new SettlementInstruction(id, accountId, ccy, ac, "BARCGB22XXX", "DEUTDEFFXXX", "DE89370400440532013000", dflt, from, to, 1, T0 + id);
    }

    /** A small, valid data set: 2 countries, 3 currencies, 4 clients (one hierarchy), 5 addresses, 5 accounts, 6 SSIs. */
    public static RefDataStoreBuilder validSet() {
        RefDataStoreBuilder b = new RefDataStoreBuilder().dataVersion(7);
        b.country(gb()); b.country(de());
        b.currency(gbp()); b.currency(eur()); b.currency(jpy());
        b.client(client(1, "C1", "LEI00000000000000001", Client.NO_PARENT));
        b.client(client(2, "C2", null, 1));
        b.client(client(3, "C3", "LEI00000000000000003", 2));
        b.client(client(10, "C10", null, Client.NO_PARENT));
        b.address(address(1, 1, AddressType.REGISTERED, true, D2020, Address.OPEN_ENDED));
        b.address(address(2, 1, AddressType.MAILING, true, D2020, D2022));
        b.address(address(3, 1, AddressType.MAILING, true, D2022, Address.OPEN_ENDED));
        b.address(address(4, 2, AddressType.REGISTERED, true, D2020, Address.OPEN_ENDED));
        b.address(address(5, 10, AddressType.BILLING, false, D2020, Address.OPEN_ENDED));
        b.account(account(100, "GB00ACC100", 1, "GBP", Account.NOT_CLOSED));
        b.account(account(101, "GB00ACC101", 1, "EUR", D2024));
        b.account(account(102, "GB00ACC102", 2, "GBP", Account.NOT_CLOSED));
        b.account(account(103, "GB00ACC103", 3, "JPY", Account.NOT_CLOSED));
        b.account(account(104, "GB00ACC104", 10, "EUR", Account.NOT_CLOSED));
        b.settlementInstruction(ssi(1000, 100, "GBP", AssetClass.CASH, true, D2020, D2022));
        b.settlementInstruction(ssi(1001, 100, "GBP", AssetClass.CASH, true, D2022, SettlementInstruction.OPEN_ENDED));
        b.settlementInstruction(ssi(1002, 100, "EUR", AssetClass.CASH, true, D2020, SettlementInstruction.OPEN_ENDED));
        b.settlementInstruction(ssi(1003, 100, "GBP", AssetClass.EQUITY, false, D2020, SettlementInstruction.OPEN_ENDED));
        b.settlementInstruction(ssi(1004, 102, "GBP", AssetClass.CASH, true, D2020, SettlementInstruction.OPEN_ENDED));
        b.settlementInstruction(ssi(1005, 104, "EUR", AssetClass.FIXED_INCOME, true, D2020, SettlementInstruction.OPEN_ENDED));
        return b;
    }
}
