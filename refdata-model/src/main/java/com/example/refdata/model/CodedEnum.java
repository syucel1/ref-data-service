package com.example.refdata.model;

/** An enumeration with a stable wire number (matches entities.proto) and a database code. */
public interface CodedEnum {
    int wireCode();
    String dbCode();
}
