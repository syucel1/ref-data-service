package com.example.refdata.client;

import java.time.Duration;
import java.util.Map;

/**
 * What a load cost: bytes and chunks received, time per {@link LoadPhase}, and how often the client had to
 * reconnect, resume or start over.
 *
 * @param bytesReceived            frame bytes read (all frames, including those later rejected)
 * @param chunksAccepted           chunks that passed seq, CRC and count checks and were staged
 * @param recordsAccepted          records staged (includes the record counts of skipped unknown-type chunks)
 * @param skippedUnknownTypeChunks chunks of entity types this build does not know (checked, contents dropped)
 * @param connections              TCP connections opened (successful or not)
 * @param queued                   whether the server queued a request for an admission slot
 * @param resumes                  snapshot requests that resumed an earlier transfer
 * @param fullRestarts             times the staging area was discarded and the snapshot requested from zero
 * @param integrityFailures        chunks rejected for a seq, CRC or record-count error
 * @param durationByPhase          wall time spent in each phase
 * @param totalDuration            wall time of the whole load
 */
public record LoadStats(long bytesReceived, long chunksAccepted, long recordsAccepted, int skippedUnknownTypeChunks,
                        int connections, boolean queued, int resumes, int fullRestarts, int integrityFailures,
                        Map<LoadPhase, Duration> durationByPhase, Duration totalDuration) {
}
