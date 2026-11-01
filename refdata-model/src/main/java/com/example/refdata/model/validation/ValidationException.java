package com.example.refdata.model.validation;

/** Thrown by the store builder when the policy is FAIL and the data has violations. */
public final class ValidationException extends RuntimeException {
    private final transient ValidationReport report;

    public ValidationException(ValidationReport report) {
        super(report.toString());
        this.report = report;
    }

    public ValidationReport report() { return report; }
}
