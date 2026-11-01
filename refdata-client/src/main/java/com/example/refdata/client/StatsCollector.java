package com.example.refdata.client;

import java.time.Duration;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

/** Accumulates the numbers behind {@link LoadStats} while a load runs. */
final class StatsCollector {
    private final long startNanos = System.nanoTime();
    private final Map<LoadPhase, Long> nanosByPhase = new EnumMap<>(LoadPhase.class);
    private LoadPhase phase;
    private long phaseStart;
    long bytesReceived;
    long chunksAccepted;
    long recordsAccepted;
    int skippedUnknownTypeChunks;
    int connections;
    boolean queued;
    int resumes;
    int fullRestarts;
    int integrityFailures;

    /** Enters a phase, attributing the time since the previous call to the previous phase. */
    void phase(LoadPhase next) {
        long now = System.nanoTime();
        if (phase != null) nanosByPhase.merge(phase, now - phaseStart, Long::sum);
        phase = next;
        phaseStart = now;
    }

    LoadPhase phase() { return phase; }

    LoadStats snapshot() {
        phase(phase);
        Map<LoadPhase, Duration> byPhase = new EnumMap<>(LoadPhase.class);
        nanosByPhase.forEach((p, n) -> byPhase.put(p, Duration.ofNanos(n)));
        return new LoadStats(bytesReceived, chunksAccepted, recordsAccepted, skippedUnknownTypeChunks, connections, queued, resumes,
                fullRestarts, integrityFailures, Collections.unmodifiableMap(byPhase), Duration.ofNanos(System.nanoTime() - startNanos));
    }
}
