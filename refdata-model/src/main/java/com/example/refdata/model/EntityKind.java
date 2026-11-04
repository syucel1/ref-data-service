package com.example.refdata.model;

/**
 * The six entity kinds, in dependency (= snapshot stream) order. The wire number equals
 * {@code refdata.schema.v1.EntityType}; the table name is the database table; the record type is the model record.
 */
public enum EntityKind {
    COUNTRY(1, "COUNTRY", Country.class),
    CURRENCY(2, "CURRENCY", Currency.class),
    CLIENT(3, "CLIENT", Client.class),
    ADDRESS(4, "ADDRESS", Address.class),
    ACCOUNT(5, "ACCOUNT", Account.class),
    SETTLEMENT_INSTRUCTION(6, "SETTLEMENT_INSTRUCTION", SettlementInstruction.class);

    private final int wireCode;
    private final String tableName;
    private final Class<?> recordType;

    EntityKind(int wireCode, String tableName, Class<?> recordType) {
        this.wireCode = wireCode;
        this.tableName = tableName;
        this.recordType = recordType;
    }

    public int wireCode() { return wireCode; }
    public String tableName() { return tableName; }
    /** The model record class of this kind. */
    public Class<?> recordType() { return recordType; }

    /** True for the kinds keyed by an ISO code (country, currency); the others have a numeric id. */
    public boolean hasCodeKey() { return this == COUNTRY || this == CURRENCY; }

    /**
     * The primary key of a record of this kind as text: the ISO code for countries and currencies, the decimal id
     * otherwise. This is the key used by change batches and by the inspection UI.
     * @throws IllegalArgumentException when the record is not of this kind
     */
    public String keyOf(Object record) {
        if (!recordType.isInstance(record)) {
            throw new IllegalArgumentException("record type " + (record == null ? "null" : record.getClass().getSimpleName()) + " differs from entity kind " + this);
        }
        return switch (this) {
            case COUNTRY -> ((Country) record).code();
            case CURRENCY -> ((Currency) record).code();
            default -> Long.toString(((HasId) record).id());
        };
    }

    /** Returns the kind for a wire number, or null when this build does not know it. */
    public static EntityKind fromWire(int code) {
        for (EntityKind k : values()) if (k.wireCode == code) return k;
        return null;
    }
}
