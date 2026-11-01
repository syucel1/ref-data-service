package com.example.refdata.model.validation;

import com.example.refdata.model.EntityKind;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/** The outcome of validating a load: violations (capped for reporting), counts per rule and per kind, rows quarantined. */
public final class ValidationReport {
    public static final int MAX_DETAILED = 100;

    private final List<Violation> detailed = new ArrayList<>();
    private final Map<Violation.Rule, Integer> countByRule = new EnumMap<>(Violation.Rule.class);
    private final Map<EntityKind, Integer> countByKind = new EnumMap<>(EntityKind.class);
    private final Map<EntityKind, Integer> quarantinedByKind = new EnumMap<>(EntityKind.class);
    private int total;

    public void add(Violation v) {
        total++;
        countByRule.merge(v.rule(), 1, Integer::sum);
        countByKind.merge(v.kind(), 1, Integer::sum);
        if (detailed.size() < MAX_DETAILED) detailed.add(v);
    }

    public void quarantined(EntityKind kind, int rows) {
        if (rows > 0) quarantinedByKind.merge(kind, rows, Integer::sum);
    }

    public boolean isClean() { return total == 0; }
    public int totalViolations() { return total; }
    /** The first {@value #MAX_DETAILED} violations, in the order found. */
    public List<Violation> violations() { return Collections.unmodifiableList(detailed); }
    public Map<Violation.Rule, Integer> countByRule() { return Collections.unmodifiableMap(countByRule); }
    public Map<EntityKind, Integer> countByKind() { return Collections.unmodifiableMap(countByKind); }
    public Map<EntityKind, Integer> quarantinedByKind() { return Collections.unmodifiableMap(quarantinedByKind); }

    @Override public String toString() {
        if (isClean()) return "validation clean";
        StringBuilder sb = new StringBuilder("validation: ").append(total).append(" violation(s) ").append(countByRule);
        if (!quarantinedByKind.isEmpty()) sb.append(", quarantined ").append(quarantinedByKind);
        for (Violation v : detailed) sb.append("\n  ").append(v);
        if (total > detailed.size()) sb.append("\n  ... and ").append(total - detailed.size()).append(" more");
        return sb.toString();
    }
}
