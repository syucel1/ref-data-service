package com.example.refdata.model;

/** A dated, typed address of a client. {@code line2}, {@code line3}, {@code region} may be null; {@code validTo == OPEN_ENDED} means no end. */
public record Address(long id, long clientId, AddressType type, String line1, String line2, String line3, String city,
                      String region, String postalCode, String countryCode, boolean primary, int validFrom, int validTo,
                      int rowVersion, long updatedAt) implements HasId {
    public static final int OPEN_ENDED = Integer.MAX_VALUE;

    /** True when the address is valid on the given epoch day (validFrom inclusive, validTo exclusive). */
    public boolean validOn(int epochDay) { return epochDay >= validFrom && epochDay < validTo; }
}
