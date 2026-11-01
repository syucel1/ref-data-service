package com.example.refdata.db;

import org.junit.Test;

import java.sql.Connection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BadDataInjectorTest {

    @Test
    public void everyInjectionLeavesTheIntendedDamage() throws Exception {
        try (EmbeddedDerby db = EmbeddedDerby.inMemory("bad-data"); Connection c = db.connect()) {
            new DataGenerator(300, 7).generate(c);
            BadDataInjector bad = new BadDataInjector(c);

            long orphan = bad.orphanAccount();
            assertEquals(0, TableDump.scalar(c, "SELECT COUNT(*) FROM ACCOUNT a JOIN CLIENT k ON a.client_id = k.client_id WHERE a.account_id = " + orphan));

            long dup = bad.duplicateAccountNumber();
            assertEquals(2, TableDump.scalar(c, "SELECT COUNT(*) FROM ACCOUNT WHERE account_number = (SELECT account_number FROM ACCOUNT WHERE account_id = " + dup + ")"));

            long nulled = bad.nullMandatoryColumn();
            assertEquals(1, TableDump.scalar(c, "SELECT COUNT(*) FROM CLIENT WHERE legal_name IS NULL AND client_id = " + nulled));

            long unknown = bad.unknownEnumCode();
            assertEquals(1, TableDump.scalar(c, "SELECT COUNT(*) FROM ACCOUNT WHERE account_type = 'CRYPTO' AND account_id = " + unknown));

            long looped = bad.hierarchyCycle();
            long parent = TableDump.scalar(c, "SELECT parent_client_id FROM CLIENT WHERE client_id = " + looped);
            assertTrue(parent > 0);

            long extended = bad.overlappingDefaultSsis();
            assertEquals(1, TableDump.scalar(c, "SELECT COUNT(*) FROM SETTLEMENT_INSTRUCTION WHERE ssi_id = " + extended + " AND effective_to IS NULL"));
        }
    }
}
