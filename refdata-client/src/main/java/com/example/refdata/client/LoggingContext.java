package com.example.refdata.client;

import org.slf4j.MDC;
import java.util.Map;

/** Captures context explicitly across library-owned threads and restores it on exit. */
final class LoggingContext implements AutoCloseable {
    private final Map<String, String> previous = MDC.getCopyOfContextMap();

    LoggingContext(Map<String, String> context) {
        MDC.clear();
        if (context != null) MDC.setContextMap(context);
    }

    @Override public void close() {
        MDC.clear();
        if (previous != null) MDC.setContextMap(previous);
    }
}
