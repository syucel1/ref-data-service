package com.example.refdata.server;

import com.example.refdata.model.Account;
import com.example.refdata.model.AccountType;
import com.example.refdata.model.Address;
import com.example.refdata.model.AddressType;
import com.example.refdata.model.AssetClass;
import com.example.refdata.model.Client;
import com.example.refdata.model.ClientType;
import com.example.refdata.model.Country;
import com.example.refdata.model.Currency;
import com.example.refdata.model.RefDataStore;
import com.example.refdata.model.RefDataStoreBuilder;
import com.example.refdata.model.SettlementInstruction;
import com.example.refdata.model.Status;

/** Hand-made stores for tests: every entity kind, every optional field both present and absent. */
public final class TestStores {
    private TestStores() {}

    /** A small valid store (3 countries, 2 currencies, 3 clients, 3 addresses, 3 accounts, 3 SSIs). */
    public static RefDataStore small() {
        return build(3, 3, 3, 3).store();
    }

    /** A valid store with the given number of clients, and as many addresses, accounts and SSIs. */
    public static RefDataStore sized(int clients) {
        return build(clients, clients, clients, clients).store();
    }

    /** {@link #small()} at the given data version (for publishing a sequence of versions). */
    public static RefDataStore small(long dataVersion) {
        return build(3, 3, 3, 3, dataVersion).store();
    }

    public static RefDataStoreBuilder.Result build(int clients, int addresses, int accounts, int ssis) {
        return build(clients, addresses, accounts, ssis, 42);
    }

    private static RefDataStoreBuilder.Result build(int clients, int addresses, int accounts, int ssis, long dataVersion) {
        RefDataStoreBuilder b = new RefDataStoreBuilder().dataVersion(dataVersion);
        b.country(new Country("GB", "GBR", 826, "United Kingdom", "Europe"));
        b.country(new Country("US", "USA", 840, "United States", null));
        b.country(new Country("DE", "DEU", 276, "Germany", "Europe"));
        b.currency(new Currency("GBP", 826, "Pound sterling", 2, true));
        b.currency(new Currency("USD", 840, "US dollar", 2, true));
        b.currency(new Currency("JPY", 392, "Yen", 0, false));
        String[] countries = {"GB", "US", "DE"};
        String[] currencies = {"GBP", "USD", "JPY"};
        for (int i = 1; i <= clients; i++) {
            boolean even = i % 2 == 0;
            b.client(new Client(i, "REF-" + i, even ? String.format("LEI%017d", i) : null, "Legal name " + i, even ? "Short " + i : null,
                    ClientType.values()[1 + i % 5], Status.values()[1 + i % 3], countries[i % 3], i > 1 && even ? i - 1 : Client.NO_PARENT,
                    i % 11, 18000 + i, i, 1_700_000_000_000L + i));
        }
        for (int i = 1; i <= addresses; i++) {
            boolean even = i % 2 == 0;
            b.address(new Address(i, 1 + (i - 1) % clients, AddressType.values()[1 + i % 4], "Line 1 of " + i, even ? "Line 2" : null,
                    even ? "Line 3" : null, "City " + i, even ? "Region" : null, "PC" + i, countries[i % 3], true, 18000 + i,
                    even ? 19000 + i : Address.OPEN_ENDED, i, 1_700_000_000_000L + i));
        }
        for (int i = 1; i <= accounts; i++) {
            boolean even = i % 2 == 0;
            b.account(new Account(i, "ACC-" + i, 1 + (i - 1) % clients, AccountType.values()[1 + i % 4], currencies[i % 3], countries[i % 3],
                    Status.values()[1 + i % 3], 18000 + i, even ? 19000 + i : Account.NOT_CLOSED, i, 1_700_000_000_000L + i));
        }
        for (int i = 1; i <= ssis; i++) {
            boolean even = i % 2 == 0;
            b.settlementInstruction(new SettlementInstruction(i, 1 + (i - 1) % accounts, currencies[i % 3], AssetClass.values()[1 + i % 3],
                    "BENEGB2LXXX", "AGNTUS33XXX", "AGENT-" + i, true, 18000 + i, even ? 19000 + i : SettlementInstruction.OPEN_ENDED,
                    i, 1_700_000_000_000L + i));
        }
        return b.build();
    }
}
