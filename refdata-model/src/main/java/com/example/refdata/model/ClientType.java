package com.example.refdata.model;

/** Legal form of a client. Code {@code 0} is UNKNOWN: the value a consumer sees for codes added by a newer schema. */
public enum ClientType implements CodedEnum {
    UNKNOWN(0, "UNKNOWN"),
    CORPORATE(1, "CORPORATE"),
    FUND(2, "FUND"),
    BANK(3, "BANK"),
    INDIVIDUAL(4, "INDIVIDUAL"),
    GOVERNMENT(5, "GOVERNMENT");

    private final int wireCode;
    private final String dbCode;

    ClientType(int wireCode, String dbCode) {
        this.wireCode = wireCode;
        this.dbCode = dbCode;
    }

    @Override public int wireCode() { return wireCode; }
    @Override public String dbCode() { return dbCode; }

    private static final ClientType[] BY_WIRE = values();

    /** Maps a wire (protobuf) number to a value; unknown numbers map to UNKNOWN, never throw. */
    public static ClientType fromWire(int code) {
        return code > 0 && code < BY_WIRE.length ? BY_WIRE[code] : UNKNOWN;
    }

    /** Maps a database code to a value; returns null for an unknown code (the validator reports it). */
    public static ClientType fromDb(String code) {
        if (code == null) return null;
        for (ClientType v : BY_WIRE) if (v != UNKNOWN && v.dbCode.equals(code)) return v;
        return null;
    }
}
