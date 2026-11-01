package com.example.refdata.model;

/** Kind of account. Code {@code 0} is UNKNOWN: the value a consumer sees for codes added by a newer schema. */
public enum AccountType implements CodedEnum {
    UNKNOWN(0, "UNKNOWN"),
    CASH(1, "CASH"),
    CUSTODY(2, "CUSTODY"),
    MARGIN(3, "MARGIN"),
    TRADING(4, "TRADING");

    private final int wireCode;
    private final String dbCode;

    AccountType(int wireCode, String dbCode) {
        this.wireCode = wireCode;
        this.dbCode = dbCode;
    }

    @Override public int wireCode() { return wireCode; }
    @Override public String dbCode() { return dbCode; }

    private static final AccountType[] BY_WIRE = values();

    /** Maps a wire (protobuf) number to a value; unknown numbers map to UNKNOWN, never throw. */
    public static AccountType fromWire(int code) {
        return code > 0 && code < BY_WIRE.length ? BY_WIRE[code] : UNKNOWN;
    }

    /** Maps a database code to a value; returns null for an unknown code (the validator reports it). */
    public static AccountType fromDb(String code) {
        if (code == null) return null;
        for (AccountType v : BY_WIRE) if (v != UNKNOWN && v.dbCode.equals(code)) return v;
        return null;
    }
}
