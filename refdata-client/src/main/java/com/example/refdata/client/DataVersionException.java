package com.example.refdata.client;

/** No host serves a data version that satisfies {@code expectedDataVersion} / {@code minDataVersion} (design row C-14). */
public final class DataVersionException extends RefDataClientException {
    public DataVersionException(String message) { super(message); }
}
