package com.example.refdata.model;

/** ISO 4217 currency. {@code code} is the three-letter primary key. */
public record Currency(String code, int numericCode, String name, int minorUnits, boolean active) {
}
