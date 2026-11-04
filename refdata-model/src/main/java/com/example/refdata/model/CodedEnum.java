package com.example.refdata.model;

/**
 * An enumeration with a stable wire number (matches entities.proto) and a database code.
 * <p>Implementations keep {@code UNKNOWN} (wire code 0) for values a newer schema may add; the static helpers below
 * hold the lookup rules once so that every coded enum behaves the same way.
 */
public interface CodedEnum {
    int wireCode();
    String dbCode();

    /**
     * The value with the given wire code, or {@code unknown} for a code this build does not know (never throws, so an
     * older consumer survives a newer server). Looks the code up rather than relying on declaration order.
     */
    static <E extends Enum<E> & CodedEnum> E fromWireCode(E[] values, int code, E unknown) {
        for (E v : values) if (v.wireCode() == code && v != unknown) return v;
        return unknown;
    }

    /** The value with the given database code, or null when the code is unknown or null (the validator reports it). */
    static <E extends Enum<E> & CodedEnum> E fromDbCode(E[] values, String code, E unknown) {
        if (code == null) return null;
        for (E v : values) if (v != unknown && v.dbCode().equals(code)) return v;
        return null;
    }
}
