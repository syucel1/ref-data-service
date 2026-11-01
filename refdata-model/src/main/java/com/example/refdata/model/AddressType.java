package com.example.refdata.model;

/** Purpose of an address. Code {@code 0} is UNKNOWN: the value a consumer sees for codes added by a newer schema. */
public enum AddressType implements CodedEnum {
    UNKNOWN(0, "UNKNOWN"),
    REGISTERED(1, "REGISTERED"),
    MAILING(2, "MAILING"),
    BILLING(3, "BILLING"),
    OPERATIONAL(4, "OPERATIONAL");

    private final int wireCode;
    private final String dbCode;

    AddressType(int wireCode, String dbCode) {
        this.wireCode = wireCode;
        this.dbCode = dbCode;
    }

    @Override public int wireCode() { return wireCode; }
    @Override public String dbCode() { return dbCode; }

    private static final AddressType[] BY_WIRE = values();

    /** Maps a wire (protobuf) number to a value; unknown numbers map to UNKNOWN, never throw. */
    public static AddressType fromWire(int code) {
        return code > 0 && code < BY_WIRE.length ? BY_WIRE[code] : UNKNOWN;
    }

    /** Maps a database code to a value; returns null for an unknown code (the validator reports it). */
    public static AddressType fromDb(String code) {
        if (code == null) return null;
        for (AddressType v : BY_WIRE) if (v != UNKNOWN && v.dbCode.equals(code)) return v;
        return null;
    }
}
