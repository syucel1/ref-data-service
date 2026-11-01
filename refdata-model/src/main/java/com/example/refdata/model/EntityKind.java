package com.example.refdata.model;

/**
 * The six entity kinds, in dependency (= snapshot stream) order. The wire number equals
 * {@code refdata.schema.v1.EntityType}; the table name is the database table.
 */
public enum EntityKind {
    COUNTRY(1, "COUNTRY"),
    CURRENCY(2, "CURRENCY"),
    CLIENT(3, "CLIENT"),
    ADDRESS(4, "ADDRESS"),
    ACCOUNT(5, "ACCOUNT"),
    SETTLEMENT_INSTRUCTION(6, "SETTLEMENT_INSTRUCTION");

    private final int wireCode;
    private final String tableName;

    EntityKind(int wireCode, String tableName) {
        this.wireCode = wireCode;
        this.tableName = tableName;
    }

    public int wireCode() { return wireCode; }
    public String tableName() { return tableName; }

    /** Returns the kind for a wire number, or null when this build does not know it. */
    public static EntityKind fromWire(int code) {
        for (EntityKind k : values()) if (k.wireCode == code) return k;
        return null;
    }
}
