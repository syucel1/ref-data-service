package com.example.refdata.db;

import org.junit.Test;

import java.sql.Connection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RefDataSchemaTest {

    @Test
    public void createsAllTablesOnInMemoryDerbyAndDropsThemAgain() throws Exception {
        try (EmbeddedDerby db = EmbeddedDerby.inMemory("schema-test"); Connection c = db.connect()) {
            RefDataSchema.create(c);
            for (String table : RefDataSchema.TABLES_IN_FK_ORDER) assertTrue(table, RefDataSchema.tableExists(c, table));

            RefDataSchema.dropIfExists(c);
            for (String table : RefDataSchema.TABLES_IN_FK_ORDER) assertFalse(table, RefDataSchema.tableExists(c, table));
            RefDataSchema.dropIfExists(c);        // a second drop is a no-op

            RefDataSchema.create(c);              // and the schema can be created again
            assertTrue(RefDataSchema.tableExists(c, "settlement_instruction"));
        }
    }

    @Test
    public void createStatementsAreTablesThenConstraints() {
        assertEquals(7, RefDataSchema.tableStatements().size());
        assertEquals(RefDataSchema.tableStatements().size() + RefDataSchema.constraintStatements().size(), RefDataSchema.createStatements().size());
        assertTrue(RefDataSchema.createStatements().get(0).startsWith("CREATE TABLE COUNTRY"));
        assertTrue(RefDataSchema.createStatements().get(7).startsWith("ALTER TABLE COUNTRY ADD CONSTRAINT PK_COUNTRY"));
    }
}
