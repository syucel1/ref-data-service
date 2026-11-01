package com.example.refdata.model;

/** ISO 3166 country. {@code code} is the two-letter primary key. */
public record Country(String code, String iso3, int numericCode, String name, String region) {
}
