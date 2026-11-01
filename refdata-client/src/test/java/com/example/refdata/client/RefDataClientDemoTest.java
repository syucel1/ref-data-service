package com.example.refdata.client;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertTrue;

/** Runs the demo main against a scripted server, so the CLI is known to work end to end. */
public class RefDataClientDemoTest {
    @Test public void demoLoadsPrintsCountsAndMeasuresLookups() throws Exception {
        try (FakeServer server = new FakeServer()) {
            server.onConnection(new SnapshotScript(SnapshotFixture.sample()));
            PrintStream original = System.out;
            ByteArrayOutputStream captured = new ByteArrayOutputStream();
            System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
            try {
                RefDataClientDemo.main(new String[] {server.endpoint()});
            } finally {
                System.setOut(original);
            }
            String out = captured.toString(StandardCharsets.UTF_8);
            assertTrue(out, out.contains("CLIENT                 3"));
            assertTrue(out, out.contains("random lookups"));
            assertTrue(out, out.contains("byRef(C-0002) -> true"));
            server.assertNoFailures();
        }
    }
}
