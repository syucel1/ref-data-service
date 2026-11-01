package com.example.refdata.server.health;

import com.example.refdata.contract.Digests;
import com.example.refdata.model.EntityKind;
import com.example.refdata.model.validation.ValidationReport;
import com.example.refdata.server.snapshot.Snapshot;

import java.util.Map;

/**
 * What startup produced (design 7.1 step 9): row counts, timings, memory, hashes, data version and the validation
 * summary. Logged once and served by {@code /startup-report}.
 *
 * @param source           database description (no credentials)
 * @param rowCounts        rows per kind in the pre-flight
 * @param loadedRows       rows per kind in the published store (after quarantine, if any)
 * @param dataVersion      release id the data was read at
 * @param loadMs           time of the database load
 * @param buildMs          time of validation and index building
 * @param encodeMs         time of snapshot encoding
 * @param validation       validation outcome
 * @param heapUsedBytes    heap in use after encoding
 * @param heapMaxBytes     -Xmx
 * @param heapEstimateBytes the pre-flight heap estimate that was checked against -Xmx (0 when there was no pre-flight)
 * @param heapAssumptions  the per-row figures and peak factor the estimate assumed
 * @param loadAttempts     how many full loads were needed
 */
public record StartupReport(String source, Map<EntityKind, Long> rowCounts, Map<EntityKind, Long> loadedRows, long dataVersion,
                            long loadMs, long buildMs, long encodeMs, ValidationReport validation, long heapUsedBytes, long heapMaxBytes,
                            long heapEstimateBytes, String heapAssumptions, int loadAttempts, Snapshot snapshot) {

    /** Multi-line text for the log. */
    public String toText() {
        StringBuilder sb = new StringBuilder("startup report\n");
        sb.append("  source:          ").append(source).append('\n');
        sb.append("  data version:    ").append(dataVersion).append('\n');
        sb.append("  snapshot id:     ").append(snapshot.snapshotIdHex()).append('\n');
        sb.append("  content sha256:  ").append(Digests.hex(snapshot.contentSha256())).append('\n');
        sb.append("  chunks:          ").append(snapshot.chunkCount()).append(" (").append(snapshot.totalUncompressedBytes()).append(" uncompressed bytes, codecs ").append(snapshot.codecs()).append(")\n");
        for (EntityKind k : EntityKind.values()) {
            sb.append(String.format("  %-24s preflight %,d  loaded %,d  chunks %d  sha256 %s%n", k, rowCounts.getOrDefault(k, 0L), loadedRows.getOrDefault(k, 0L),
                    snapshot.manifest(k).getChunkCount(), Digests.hex(snapshot.entityHash(k))));
        }
        sb.append("  timings:         load ").append(loadMs).append(" ms (").append(loadAttempts).append(" attempt(s)), build ").append(buildMs).append(" ms, encode ").append(encodeMs).append(" ms\n");
        sb.append("  heap:            ").append(heapUsedBytes >> 20).append(" MiB used of ").append(heapMaxBytes >> 20).append(" MiB; pre-flight estimate ")
                .append(heapEstimateBytes >> 20).append(" MiB assuming ").append(heapAssumptions).append('\n');
        sb.append("  validation:      ").append(validation.isClean() ? "clean" : validation.totalViolations() + " violation(s) " + validation.countByRule()
                + (validation.quarantinedByKind().isEmpty() ? "" : ", quarantined " + validation.quarantinedByKind()));
        return sb.toString();
    }

    /** The same as JSON. */
    public String toJson() {
        Json j = new Json();
        j.obj().str("source", source).num("dataVersion", dataVersion).str("snapshotId", snapshot.snapshotIdHex())
                .str("contentSha256", Digests.hex(snapshot.contentSha256())).num("chunks", snapshot.chunkCount())
                .num("totalRecords", snapshot.totalRecords()).num("totalUncompressedBytes", snapshot.totalUncompressedBytes());
        j.key("entities").arr();
        for (EntityKind k : EntityKind.values()) {
            j.obj().str("kind", k.name()).num("preflightRows", rowCounts.getOrDefault(k, 0L)).num("loadedRows", loadedRows.getOrDefault(k, 0L))
                    .num("chunks", snapshot.manifest(k).getChunkCount()).num("firstSeq", snapshot.manifest(k).getFirstSeq())
                    .str("sha256", Digests.hex(snapshot.entityHash(k))).end();
        }
        j.end();
        j.key("timingsMs").obj().num("load", loadMs).num("build", buildMs).num("encode", encodeMs).num("loadAttempts", loadAttempts).end();
        j.key("heap").obj().num("usedBytes", heapUsedBytes).num("maxBytes", heapMaxBytes).num("estimateBytes", heapEstimateBytes).str("estimateAssumptions", heapAssumptions).end();
        j.key("validation").obj().bool("clean", validation.isClean()).num("violations", validation.totalViolations());
        j.key("byRule").obj();
        validation.countByRule().forEach((r, n) -> j.num(r.name(), n));
        j.end();
        j.key("quarantined").obj();
        validation.quarantinedByKind().forEach((k, n) -> j.num(k.name(), n));
        j.end().end();
        return j.end().toString();
    }
}
