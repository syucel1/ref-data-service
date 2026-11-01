package com.example.refdata.model;

/** Asset class a settlement instruction applies to. Code {@code 0} is UNKNOWN: the value a consumer sees for codes added by a newer schema. */
public enum AssetClass implements CodedEnum {
    UNKNOWN(0, "UNKNOWN"),
    CASH(1, "CASH"),
    EQUITY(2, "EQUITY"),
    FIXED_INCOME(3, "FIXED_INCOME");

    private final int wireCode;
    private final String dbCode;

    AssetClass(int wireCode, String dbCode) {
        this.wireCode = wireCode;
        this.dbCode = dbCode;
    }

    @Override public int wireCode() { return wireCode; }
    @Override public String dbCode() { return dbCode; }

    private static final AssetClass[] BY_WIRE = values();

    /** Maps a wire (protobuf) number to a value; unknown numbers map to UNKNOWN, never throw. */
    public static AssetClass fromWire(int code) {
        return code > 0 && code < BY_WIRE.length ? BY_WIRE[code] : UNKNOWN;
    }

    /** Maps a database code to a value; returns null for an unknown code (the validator reports it). */
    public static AssetClass fromDb(String code) {
        if (code == null) return null;
        for (AssetClass v : BY_WIRE) if (v != UNKNOWN && v.dbCode.equals(code)) return v;
        return null;
    }
}
