package com.example.refdata.loader.jdbc;

import com.example.refdata.model.EntityKind;
import com.example.refdata.model.RefDataSource;
import com.example.refdata.model.RefDataStoreBuilder;
import com.example.refdata.testing.IntegrationTest;
import org.junit.Assume;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Loads the generated MEDIUM database (data/medium at the repository root) end to end and reports the timing. */
@Category(IntegrationTest.class)
public class MediumDatabaseLoadIT {
    private static final Logger log = LoggerFactory.getLogger(MediumDatabaseLoadIT.class);

    @Test
    public void loadsAndValidatesTheMediumDatabase() throws Exception {
        Path dir = Path.of(System.getProperty("refdata.medium.dir", "../data/medium")).toAbsolutePath().normalize();
        Assume.assumeTrue("no generated database at " + dir, Files.isDirectory(dir.resolve("seg0")));

        JdbcRefDataSource source = new JdbcRefDataSource("jdbc:derby:" + dir, JdbcLoadConfig.defaults());
        RefDataSource.Preflight preflight = source.preflight();
        RefDataStoreBuilder builder = new RefDataStoreBuilder();
        preflight.rowCounts().forEach(builder::expect);

        long start = System.nanoTime();
        long version = source.load(builder);
        RefDataStoreBuilder.Result result = builder.dataVersion(version).build();
        log.info("medium: loaded and built in {} ms", (System.nanoTime() - start) / 1_000_000);

        assertTrue(result.report().toString(), result.report().isClean());
        assertEquals(preflight.dataVersion(), version);
        for (EntityKind k : EntityKind.values()) assertEquals(k.toString(), (long) preflight.rowCounts().get(k), result.store().count(k));
        assertTrue(result.store().count(EntityKind.CLIENT) >= 100_000);
    }
}
