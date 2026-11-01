package com.example.refdata.model;

/**
 * A client (legal entity or person). Dates are epoch days, timestamps epoch milliseconds (UTC).
 * {@code parentClientId == NO_PARENT} means the client is at the top of its hierarchy; {@code lei} may be null.
 */
public record Client(long id, String ref, String lei, String legalName, String shortName, ClientType type, Status status,
                     String incorporationCountry, long parentClientId, int riskRating, int onboardedDate,
                     int rowVersion, long updatedAt) implements HasId {
    public static final long NO_PARENT = 0L;

    public boolean hasParent() { return parentClientId != NO_PARENT; }
}
