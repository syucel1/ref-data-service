package com.example.refdata.model;

/** Where an account settles a currency and asset class, effective from/to (epoch days; {@code effectiveTo == OPEN_ENDED} = no end). */
public record SettlementInstruction(long id, long accountId, String currencyCode, AssetClass assetClass, String beneficiaryBic,
                                    String agentBic, String agentAccount, boolean isDefault, int effectiveFrom, int effectiveTo,
                                    int rowVersion, long updatedAt) implements HasId {
    public static final int OPEN_ENDED = Integer.MAX_VALUE;

    /** True when the instruction is effective on the given epoch day (from inclusive, to exclusive). */
    public boolean effectiveOn(int epochDay) { return epochDay >= effectiveFrom && epochDay < effectiveTo; }
}
