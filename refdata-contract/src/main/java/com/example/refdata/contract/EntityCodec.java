package com.example.refdata.contract;

import com.example.refdata.model.Account;
import com.example.refdata.model.AccountType;
import com.example.refdata.model.Address;
import com.example.refdata.model.AddressType;
import com.example.refdata.model.AssetClass;
import com.example.refdata.model.Client;
import com.example.refdata.model.ClientType;
import com.example.refdata.model.Country;
import com.example.refdata.model.Currency;
import com.example.refdata.model.EntityKind;
import com.example.refdata.model.SettlementInstruction;
import com.example.refdata.model.Status;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Converts between the lean model records and the generated protobuf messages. Optional proto fields map to the
 * model's sentinels ({@code NO_PARENT}, {@code OPEN_ENDED}, {@code NOT_CLOSED}, null strings). Unknown enum numbers
 * from a newer server become {@code UNKNOWN}.
 */
public final class EntityCodec {
    private EntityCodec() {}

    // ---------------------------------------------------------------- model -> proto
    public static com.example.refdata.schema.v1.Country toProto(Country c) {
        return com.example.refdata.schema.v1.Country.newBuilder().setCountryCode(c.code()).setIso3(c.iso3()).setNumericCode(c.numericCode())
                .setName(c.name()).setRegion(nz(c.region())).build();
    }

    public static com.example.refdata.schema.v1.Currency toProto(Currency c) {
        return com.example.refdata.schema.v1.Currency.newBuilder().setCurrencyCode(c.code()).setNumericCode(c.numericCode()).setName(c.name())
                .setMinorUnits(c.minorUnits()).setActive(c.active()).build();
    }

    public static com.example.refdata.schema.v1.Client toProto(Client c) {
        com.example.refdata.schema.v1.Client.Builder b = com.example.refdata.schema.v1.Client.newBuilder().setClientId(c.id()).setClientRef(c.ref()).setLegalName(c.legalName())
                .setShortName(nz(c.shortName())).setClientTypeValue(c.type().wireCode()).setStatusValue(c.status().wireCode())
                .setIncorporationCountry(c.incorporationCountry()).setRiskRating(c.riskRating()).setOnboardedDate(c.onboardedDate())
                .setRowVersion(c.rowVersion()).setUpdatedAt(c.updatedAt());
        if (c.lei() != null) b.setLei(c.lei());
        if (c.hasParent()) b.setParentClientId(c.parentClientId());
        return b.build();
    }

    public static com.example.refdata.schema.v1.Address toProto(Address a) {
        com.example.refdata.schema.v1.Address.Builder b = com.example.refdata.schema.v1.Address.newBuilder().setAddressId(a.id()).setClientId(a.clientId())
                .setAddressTypeValue(a.type().wireCode()).setLine1(a.line1()).setCity(a.city()).setPostalCode(a.postalCode())
                .setCountryCode(a.countryCode()).setIsPrimary(a.primary()).setValidFrom(a.validFrom())
                .setRowVersion(a.rowVersion()).setUpdatedAt(a.updatedAt());
        if (a.line2() != null) b.setLine2(a.line2());
        if (a.line3() != null) b.setLine3(a.line3());
        if (a.region() != null) b.setRegion(a.region());
        if (a.validTo() != Address.OPEN_ENDED) b.setValidTo(a.validTo());
        return b.build();
    }

    public static com.example.refdata.schema.v1.Account toProto(Account a) {
        com.example.refdata.schema.v1.Account.Builder b = com.example.refdata.schema.v1.Account.newBuilder().setAccountId(a.id()).setAccountNumber(a.number()).setClientId(a.clientId())
                .setAccountTypeValue(a.type().wireCode()).setBaseCurrency(a.baseCurrency()).setBookingCountry(a.bookingCountry())
                .setStatusValue(a.status().wireCode()).setOpenedDate(a.openedDate()).setRowVersion(a.rowVersion()).setUpdatedAt(a.updatedAt());
        if (a.isClosed()) b.setClosedDate(a.closedDate());
        return b.build();
    }

    public static com.example.refdata.schema.v1.SettlementInstruction toProto(SettlementInstruction s) {
        com.example.refdata.schema.v1.SettlementInstruction.Builder b = com.example.refdata.schema.v1.SettlementInstruction.newBuilder().setSsiId(s.id()).setAccountId(s.accountId())
                .setCurrencyCode(s.currencyCode()).setAssetClassValue(s.assetClass().wireCode()).setBeneficiaryBic(s.beneficiaryBic())
                .setAgentBic(s.agentBic()).setAgentAccount(s.agentAccount()).setIsDefault(s.isDefault()).setEffectiveFrom(s.effectiveFrom())
                .setRowVersion(s.rowVersion()).setUpdatedAt(s.updatedAt());
        if (s.effectiveTo() != SettlementInstruction.OPEN_ENDED) b.setEffectiveTo(s.effectiveTo());
        return b.build();
    }

    // ---------------------------------------------------------------- proto -> model
    public static Country fromProto(com.example.refdata.schema.v1.Country p) {
        return new Country(p.getCountryCode(), p.getIso3(), p.getNumericCode(), p.getName(), en(p.getRegion()));
    }

    public static Currency fromProto(com.example.refdata.schema.v1.Currency p) {
        return new Currency(p.getCurrencyCode(), p.getNumericCode(), p.getName(), p.getMinorUnits(), p.getActive());
    }

    public static Client fromProto(com.example.refdata.schema.v1.Client p) {
        return new Client(p.getClientId(), p.getClientRef(), p.hasLei() ? p.getLei() : null, p.getLegalName(), en(p.getShortName()),
                ClientType.fromWire(p.getClientTypeValue()), Status.fromWire(p.getStatusValue()), p.getIncorporationCountry(),
                p.hasParentClientId() ? p.getParentClientId() : Client.NO_PARENT, p.getRiskRating(), p.getOnboardedDate(),
                p.getRowVersion(), p.getUpdatedAt());
    }

    public static Address fromProto(com.example.refdata.schema.v1.Address p) {
        return new Address(p.getAddressId(), p.getClientId(), AddressType.fromWire(p.getAddressTypeValue()), p.getLine1(),
                p.hasLine2() ? p.getLine2() : null, p.hasLine3() ? p.getLine3() : null, p.getCity(), p.hasRegion() ? p.getRegion() : null,
                p.getPostalCode(), p.getCountryCode(), p.getIsPrimary(), p.getValidFrom(), p.hasValidTo() ? p.getValidTo() : Address.OPEN_ENDED,
                p.getRowVersion(), p.getUpdatedAt());
    }

    public static Account fromProto(com.example.refdata.schema.v1.Account p) {
        return new Account(p.getAccountId(), p.getAccountNumber(), p.getClientId(), AccountType.fromWire(p.getAccountTypeValue()),
                p.getBaseCurrency(), p.getBookingCountry(), Status.fromWire(p.getStatusValue()), p.getOpenedDate(),
                p.hasClosedDate() ? p.getClosedDate() : Account.NOT_CLOSED, p.getRowVersion(), p.getUpdatedAt());
    }

    public static SettlementInstruction fromProto(com.example.refdata.schema.v1.SettlementInstruction p) {
        return new SettlementInstruction(p.getSsiId(), p.getAccountId(), p.getCurrencyCode(), AssetClass.fromWire(p.getAssetClassValue()),
                p.getBeneficiaryBic(), p.getAgentBic(), p.getAgentAccount(), p.getIsDefault(), p.getEffectiveFrom(),
                p.hasEffectiveTo() ? p.getEffectiveTo() : SettlementInstruction.OPEN_ENDED, p.getRowVersion(), p.getUpdatedAt());
    }

    // ---------------------------------------------------------------- one record of any kind <-> bytes
    /** Serializes one model record of the given kind as its protobuf message. */
    public static byte[] toBytes(EntityKind kind, Object record) {
        return switch (kind) {
            case COUNTRY -> toProto((Country) record).toByteArray();
            case CURRENCY -> toProto((Currency) record).toByteArray();
            case CLIENT -> toProto((Client) record).toByteArray();
            case ADDRESS -> toProto((Address) record).toByteArray();
            case ACCOUNT -> toProto((Account) record).toByteArray();
            case SETTLEMENT_INSTRUCTION -> toProto((SettlementInstruction) record).toByteArray();
        };
    }

    /** The inverse of {@link #toBytes}. */
    public static Object fromBytes(EntityKind kind, byte[] bytes) throws InvalidProtocolBufferException {
        return switch (kind) {
            case COUNTRY -> fromProto(com.example.refdata.schema.v1.Country.parseFrom(bytes));
            case CURRENCY -> fromProto(com.example.refdata.schema.v1.Currency.parseFrom(bytes));
            case CLIENT -> fromProto(com.example.refdata.schema.v1.Client.parseFrom(bytes));
            case ADDRESS -> fromProto(com.example.refdata.schema.v1.Address.parseFrom(bytes));
            case ACCOUNT -> fromProto(com.example.refdata.schema.v1.Account.parseFrom(bytes));
            case SETTLEMENT_INSTRUCTION -> fromProto(com.example.refdata.schema.v1.SettlementInstruction.parseFrom(bytes));
        };
    }

    private static String nz(String s) { return s == null ? "" : s; }
    private static String en(String s) { return s == null || s.isEmpty() ? null : s; }
}
