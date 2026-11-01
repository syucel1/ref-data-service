package com.example.refdata.client;

/** Base class of everything {@link RefDataClient#load()} can throw. */
public class RefDataClientException extends RuntimeException {
    public RefDataClientException(String message) { super(message); }
    public RefDataClientException(String message, Throwable cause) { super(message, cause); }
}
