package com.example.refdata.server.admin;

import com.example.refdata.model.EntityKind;
import java.util.Map;

/**
 * What a full reload published: the data version before and after (equal when a direct SQL fix did not add a release
 * row), the new snapshot id, the row counts loaded, and the time spent loading/validating and in total.
 */
public record ReloadResult(long previousDataVersion, long dataVersion, String snapshotId, Map<EntityKind, Long> rows, long loadMs, long totalMs) {
    public ReloadResult {
        rows = Map.copyOf(rows);
    }

    /** The {@code POST /reload} response body. */
    public String toJson() {
        StringBuilder sb = new StringBuilder(256).append('{')
                .append("\"previousDataVersion\":").append(previousDataVersion)
                .append(",\"dataVersion\":").append(dataVersion)
                .append(",\"snapshotId\":\"").append(snapshotId).append('"')
                .append(",\"rows\":{");
        boolean first = true;
        for (EntityKind k : EntityKind.values()) {
            Long n = rows.get(k);
            if (n == null) continue;
            if (!first) sb.append(',');
            first = false;
            sb.append('"').append(k.tableName()).append("\":").append(n);
        }
        return sb.append("},\"loadMs\":").append(loadMs).append(",\"totalMs\":").append(totalMs).append('}').toString();
    }
}
