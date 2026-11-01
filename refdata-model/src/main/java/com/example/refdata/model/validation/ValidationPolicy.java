package com.example.refdata.model.validation;

/** What to do when the loaded data breaks a rule. */
public enum ValidationPolicy {
    /** Any violation fails the build (default). */
    FAIL,
    /** Offending rows and the rows that depend on them are left out; the rest is published. */
    QUARANTINE
}
