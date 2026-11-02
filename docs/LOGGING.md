# Logging

Application code uses SLF4J 2.0.17. The standalone server, client demo and tests select
Log4j 2.25.4 through `log4j-slf4j2-impl`. Dependencies remain bundled under `lib/`.

## Embedding the client

The reusable `refdata-client` runtime includes the SLF4J API, but does not select a backend.
Use the consuming application's existing SLF4J 2 provider and logging configuration.
For a standalone application using this repository's Log4j setup, include these runtime jars:

- `log4j-api-2.25.4.jar`
- `log4j-core-2.25.4.jar`
- `log4j-slf4j2-impl-2.25.4.jar`

Choose one SLF4J provider for the application. Do not combine this binding with another
provider or with `log4j-to-slf4j`. Without a provider, SLF4J reports its usual startup warning
and discards log events; cache loading and lookups still function.

The demo explicitly supplies Log4j without adding it to the reusable client runtime:

```text
gradle :refdata-client:demo -Pargs="localhost:7400 --lz4"
gradle :refdata-client:verifyLoggingRuntime
```

The runtime verification also runs as part of `:refdata-client:check`.

## Console output and log levels

The default server/demo configuration is `config/log4j2.xml`: synchronous stdout logging
with timestamp, level, thread, logger, MDC context and message. Root level is INFO,
client and session loggers are INFO, and Netty is WARN. Tests use `config/log4j2-test.xml`.

Override the supplied configuration's levels using JVM system properties:

| Property | Default | Scope |
|---|---|---|
| `rds.log.level` | `INFO` | Root and other inherited loggers |
| `rds.log.client.level` | `INFO` | `com.example.refdata.client` |
| `rds.log.sessions.level` | `INFO` | `com.example.refdata.server.sessions` |
| `rds.log.netty.level` | `WARN` | `io.netty` |

The client, sessions and Netty have explicit levels: changing only the root level does
not override them. For other packages, use a custom configuration with a named Logger.

```text
gradle -Drds.log.client.level=DEBUG :refdata-client:demo -Pargs="localhost:7400"
gradle -Drds.log.sessions.level=DEBUG :refdata-server:run --args="config/refdata-server.properties"
```

Both Gradle entry points forward logging overrides. With a distribution launcher or direct
`java` invocation, supply the same properties as application JVM options (before the main
class). For generated distribution scripts, use `JAVA_OPTS` for overrides rather than
editing the generated script. Configuration examples assume the repository working directory;
use absolute paths from other directories.

## Optional rolling files

Opt in to console **and** rotating file output:

```text
gradle -Dlog4j2.configurationFile=config/log4j2-rolling.xml -Drds.log.dir=build/logs :refdata-server:run --args="config/refdata-server.properties"
```

The active file is `refdata.log`; rollover triggers at 10 MB, retaining at most seven
compressed archives (`refdata-1.log.gz` through `refdata-7.log.gz`). The active file plus
seven uncompressed archive equivalents is nominally about 80 MB; individual events can
exceed the threshold, and compression can require temporary disk space. This is bounded
archive retention, not a hard disk quota. The directory defaults to `logs` relative to the
application working directory. Give separate processes separate directories to avoid
multiple JVMs rotating the same file. Logging remains synchronous.

## Diagnostic context and error ownership

Client load threads copy the caller's SLF4J MDC and add these namespaced fields:

| Field | Meaning |
|---|---|
| `rds.client` / `rds.instance` | Configured application and instance names |
| `rds.load` | Unique loader thread name within the JVM |
| `rds.endpoint` | Current server endpoint |
| `rds.session` | Session ID announced by the server, for correlation with server logs |
| `rds.snapshot` | Snapshot ID, once announced by the server |
| `rds.request` | Request ID within the current load |

Keep-alive workers explicitly inherit the load context. Library-owned worker scopes restore
their previous MDC when they exit; the initiating application's MDC is not modified.
The `rds.*` MDC namespace is reserved for this library. Host MDC values appear in `%X` too,
so applications should avoid putting secrets or record contents in their logging context.

Server session diagnostics already include session, client, snapshot and transfer identifiers
as message fields; these do not depend on thread-local state across Netty callbacks.
Session close summaries use the dedicated `com.example.refdata.server.sessions` logger.

Expected load failures are summarised at WARN and remain available through the thrown
exception or failed future. Unexpected terminal failures include a stack trace at ERROR;
unexpected errors retried by the loader include a stack trace at WARN. The server launcher
logs a fatal startup failure once instead of also printing the same message to stderr.
CLI usage instructions and demo results still use stdout/stderr. Successful record lookups
do not log and remain free of logging context work.
