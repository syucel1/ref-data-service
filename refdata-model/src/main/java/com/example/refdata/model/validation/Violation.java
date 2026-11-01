package com.example.refdata.model.validation;

import com.example.refdata.model.EntityKind;

/** One broken rule on one row (or on a table, when {@code id} is {@code -1}). */
public record Violation(Rule rule, EntityKind kind, long id, String detail) {
    public enum Rule {
        MANDATORY_MISSING, INVALID_VALUE, DUPLICATE_KEY, MISSING_REFERENCE, HIERARCHY_CYCLE,
        INVALID_DATE_RANGE, OVERLAPPING_PERIODS, TOO_FEW_ROWS
    }

    @Override public String toString() {
        return rule + " " + kind + (id >= 0 ? "#" + id : "") + ": " + detail;
    }
}
