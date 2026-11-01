package com.example.refdata.server.health;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Process-wide counters and gauges, updated from the I/O threads without locks and rendered as Prometheus-style
 * text by {@link #render()}.
 */
public final class ServerMetrics {
    private final LongAdder sessionsActive = new LongAdder();
    private final LongAdder sessionsTotal = new LongAdder();
    private final LongAdder sessionsQueued = new LongAdder();
    private final LongAdder connectionsRejected = new LongAdder();
    private final LongAdder transfersActive = new LongAdder();
    private final LongAdder transfersStarted = new LongAdder();
    private final LongAdder transfersCompleted = new LongAdder();
    private final LongAdder transfersNotModified = new LongAdder();
    private final LongAdder transfersResumed = new LongAdder();
    private final LongAdder serverBusyReplies = new LongAdder();
    private final LongAdder bytesSent = new LongAdder();
    private final LongAdder chunksSent = new LongAdder();
    private final LongAdder integrityReports = new LongAdder();
    private final LongAdder integrityFailuresConfirmed = new LongAdder();
    private final LongAdder sessionErrors = new LongAdder();
    private final ConcurrentMap<String, LongAdder> transfersFailedByReason = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, LongAdder> sessionsByClient = new ConcurrentHashMap<>();
    /** Client names are client-supplied: beyond this many distinct names the rest are counted as {@value #OTHER_CLIENT}. */
    static final int MAX_CLIENT_NAMES = 1_000;
    static final String OTHER_CLIENT = "other";
    private final ConcurrentMap<String, LongAdder> sessionsByOutcome = new ConcurrentHashMap<>();

    public void sessionOpened() { sessionsActive.increment(); sessionsTotal.increment(); }
    public void sessionClosed(String clientName, String outcome) {
        sessionsActive.decrement();
        String name = clientName == null || clientName.isEmpty() ? "unknown" : clientName;
        if (!sessionsByClient.containsKey(name) && sessionsByClient.size() >= MAX_CLIENT_NAMES) name = OTHER_CLIENT;
        count(sessionsByClient, name);
        count(sessionsByOutcome, outcome);
    }
    public void connectionRejected() { connectionsRejected.increment(); }
    public void queued() { sessionsQueued.increment(); }
    public void dequeued() { sessionsQueued.decrement(); }
    public void serverBusy() { serverBusyReplies.increment(); }
    public void transferStarted(boolean resume) { transfersActive.increment(); transfersStarted.increment(); if (resume) transfersResumed.increment(); }
    public void transferCompleted() { transfersActive.decrement(); transfersCompleted.increment(); }
    public void transferFailed(String reason) { transfersActive.decrement(); count(transfersFailedByReason, reason); }
    public void notModified() { transfersNotModified.increment(); }
    public void chunkSent(long bytes) { chunksSent.increment(); bytesSent.add(bytes); }
    public void controlBytesSent(long bytes) { bytesSent.add(bytes); }
    public void integrityReport(boolean confirmed) { integrityReports.increment(); if (confirmed) integrityFailuresConfirmed.increment(); }
    public void sessionError() { sessionErrors.increment(); }

    public long sessionsActive() { return sessionsActive.sum(); }
    public long sessionsQueued() { return sessionsQueued.sum(); }
    public long transfersActive() { return transfersActive.sum(); }
    public long transfersStarted() { return transfersStarted.sum(); }
    public long transfersCompleted() { return transfersCompleted.sum(); }
    public long bytesSent() { return bytesSent.sum(); }
    public long integrityReports() { return integrityReports.sum(); }
    public long serverBusyReplies() { return serverBusyReplies.sum(); }
    public long connectionsRejected() { return connectionsRejected.sum(); }
    public long transfersFailed(String reason) { LongAdder a = transfersFailedByReason.get(reason); return a == null ? 0 : a.sum(); }
    public long sessions(String clientName) { LongAdder a = sessionsByClient.get(clientName); return a == null ? 0 : a.sum(); }

    /** Prometheus text exposition (gauges and counters, one sample per line). */
    public String render() {
        StringBuilder sb = new StringBuilder(2048);
        gauge(sb, "rds_sessions_active", sessionsActive.sum());
        gauge(sb, "rds_sessions_queued", sessionsQueued.sum());
        gauge(sb, "rds_transfers_active", transfersActive.sum());
        counter(sb, "rds_sessions_total", sessionsTotal.sum());
        counter(sb, "rds_connections_rejected_total", connectionsRejected.sum());
        counter(sb, "rds_transfers_started_total", transfersStarted.sum());
        counter(sb, "rds_transfers_completed_total", transfersCompleted.sum());
        counter(sb, "rds_transfers_resumed_total", transfersResumed.sum());
        counter(sb, "rds_transfers_not_modified_total", transfersNotModified.sum());
        counter(sb, "rds_server_busy_total", serverBusyReplies.sum());
        counter(sb, "rds_bytes_sent_total", bytesSent.sum());
        counter(sb, "rds_chunks_sent_total", chunksSent.sum());
        counter(sb, "rds_integrity_reports_total", integrityReports.sum());
        counter(sb, "rds_integrity_failures_confirmed_total", integrityFailuresConfirmed.sum());
        counter(sb, "rds_session_errors_total", sessionErrors.sum());
        labelled(sb, "rds_transfers_failed_total", "reason", transfersFailedByReason);
        labelled(sb, "rds_sessions_by_client_total", "client", sessionsByClient);
        labelled(sb, "rds_sessions_by_outcome_total", "outcome", sessionsByOutcome);
        return sb.toString();
    }

    private static void count(ConcurrentMap<String, LongAdder> map, String key) {
        map.computeIfAbsent(key, k -> new LongAdder()).increment();
    }

    private static void gauge(StringBuilder sb, String name, long v) {
        sb.append("# TYPE ").append(name).append(" gauge\n").append(name).append(' ').append(v).append('\n');
    }

    private static void counter(StringBuilder sb, String name, long v) {
        sb.append("# TYPE ").append(name).append(" counter\n").append(name).append(' ').append(v).append('\n');
    }

    private static void labelled(StringBuilder sb, String name, String label, ConcurrentMap<String, LongAdder> values) {
        sb.append("# TYPE ").append(name).append(" counter\n");
        for (Map.Entry<String, LongAdder> e : new TreeMap<>(values).entrySet()) {
            sb.append(name).append('{').append(label).append("=\"").append(e.getKey().replace("\\", "\\\\").replace("\"", "\\\"")).append("\"} ")
                    .append(e.getValue().sum()).append('\n');
        }
    }
}
