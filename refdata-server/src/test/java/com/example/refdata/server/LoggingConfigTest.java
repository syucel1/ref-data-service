package com.example.refdata.server;

import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.appender.rolling.CompositeTriggeringPolicy;
import org.apache.logging.log4j.core.appender.rolling.SizeBasedTriggeringPolicy;
import org.apache.logging.log4j.core.config.xml.XmlConfiguration;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import java.nio.file.Files;
import java.nio.file.Path;
import static org.junit.Assert.*;

public class LoggingConfigTest {
    @Rule public TemporaryFolder temp = new TemporaryFolder();

    @Test public void optionalFileConfigurationWritesAndRotatesWithBoundedRetention() throws Exception {
        String previous = System.getProperty("rds.log.dir");
        Path directory = temp.newFolder("logs").toPath();
        System.setProperty("rds.log.dir", directory.toString());
        try (LoggerContext context = new LoggerContext("rolling-config-test")) {
            try (var input = Files.newInputStream(Path.of("../config/log4j2-rolling.xml"))) {
                context.start(new XmlConfiguration(context, new ConfigurationSource(input)));
            }
            RollingFileAppender file = context.getConfiguration().getAppender("File");
            assertNotNull(file);
            CompositeTriggeringPolicy policies = file.getTriggeringPolicy();
            SizeBasedTriggeringPolicy size = (SizeBasedTriggeringPolicy) policies.getTriggeringPolicies()[0];
            assertEquals(10 * 1024 * 1024, size.getMaxFileSize());
            for (int i = 0; i < 9; i++) {
                context.getLogger("com.example.refdata.server.sessions").info("rotation-test-{}", i);
                file.getManager().rollover();
            }
            context.getLogger("com.example.refdata.server.sessions").info("active-file-marker");
        } finally {
            if (previous == null) System.clearProperty("rds.log.dir"); else System.setProperty("rds.log.dir", previous);
        }
        assertTrue(Files.readString(directory.resolve("refdata.log")).contains("active-file-marker"));
        try (var files = Files.list(directory)) {
            long archives = files.filter(p -> p.toString().endsWith(".log.gz")).count();
            assertEquals(7, archives);
        }
    }
}
