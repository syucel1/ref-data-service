package com.example.refdata.model;

/** Life-cycle status of a client or account. Code {@code 0} is UNKNOWN: the value a consumer sees for codes added by a newer schema. */
public enum Status implements CodedEnum {
    UNKNOWN(0, "UNKNOWN"),
    ACTIVE(1, "ACTIVE"),
    SUSPENDED(2, "SUSPENDED"),
    CLOSED(3, "CLOSED");

    private final int wireCode;
    private final String dbCode;

    Status(int wireCode, String dbCode) {
        this.wireCode = wireCode;
        this.dbCode = dbCode;
    }

    @Override public int wireCode() { return wireCode; }
    @Override public String dbCode() { return dbCode; }

    private static final Status[] BY_WIRE = values();

    /** Maps a wire (protobuf) number to a value; unknown numbers map to UNKNOWN, never throw. */
    public static Status fromWire(int code) {
        return code > 0 && code < BY_WIRE.length ? BY_WIRE[code] : UNKNOWN;
    }

    /** Maps a database code to a value; returns null for an unknown code (the validator reports it). */
    public static Status fromDb(String code) {
        if (code == null) return null;
        for (Status v : BY_WIRE) if (v != UNKNOWN && v.dbCode.equals(code)) return v;
        return null;
    }
}
