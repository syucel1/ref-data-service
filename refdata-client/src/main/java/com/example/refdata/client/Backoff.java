package com.example.refdata.client;

import java.time.Duration;
import java.util.Random;

/** Exponential back-off with jitter: each delay is drawn from [base/2, base], base doubling up to the maximum. */
final class Backoff {
    private final long initialMs;
    private final long maxMs;
    private final Random random;
    private int attempt;

    Backoff(Duration initial, Duration max, Random random) {
        this.initialMs = Math.max(1, initial.toMillis());
        this.maxMs = Math.max(this.initialMs, max.toMillis());
        this.random = random;
    }

    /** The delay to wait before the next attempt; grows with every call until {@link #reset()}. */
    Duration next() {
        long base = attempt >= 30 ? maxMs : Math.min(maxMs, initialMs << attempt);
        attempt++;
        long half = base / 2;
        return Duration.ofMillis(half + (long) (random.nextDouble() * (base - half + 1)));
    }

    /** Called after progress is made, so that the next failure starts small again. */
    void reset() { attempt = 0; }
}
