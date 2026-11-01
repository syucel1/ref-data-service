package com.example.refdata.client;

import java.time.Duration;

/** The current connection is unusable (GoAway, server-side error, bad resume answer); reconnect and carry on. */
final class Reconnect extends Exception {
    private final boolean nextHost;
    private final Duration delay;

    /**
     * @param nextHost whether to move on to the next host in the list rather than retry this one
     * @param delay    the server's reconnect hint, or null to use the back-off
     */
    Reconnect(String message, boolean nextHost, Duration delay) {
        super(message);
        this.nextHost = nextHost;
        this.delay = delay;
    }

    boolean nextHost() { return nextHost; }
    Duration delay() { return delay; }
}
