package com.example.refdata.client;

import java.time.Duration;

/** The server answered SERVER_BUSY; {@link #retryAfter()} is its hint (zero when it gave none). The client retries until its startup timeout. */
public final class ServerBusyException extends RefDataClientException {
    private final Duration retryAfter;

    public ServerBusyException(String message, Duration retryAfter) {
        super(message);
        this.retryAfter = retryAfter;
    }

    public Duration retryAfter() { return retryAfter; }
}
