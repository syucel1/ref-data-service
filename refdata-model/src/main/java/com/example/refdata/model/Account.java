package com.example.refdata.model;

/** An account of a client. {@code closedDate == NOT_CLOSED} when the account is open. */
public record Account(long id, String number, long clientId, AccountType type, String baseCurrency, String bookingCountry,
                      Status status, int openedDate, int closedDate, int rowVersion, long updatedAt) implements HasId {
    public static final int NOT_CLOSED = Integer.MAX_VALUE;

    public boolean isClosed() { return closedDate != NOT_CLOSED; }
}
