# Reference Data Service (Java) — version 0.1

A static/reference data service for low-latency consumers. At startup it loads six tables from a relational
database, validates them, builds an immutable indexed in-memory model, encodes it once into Protocol Buffers
chunks and publishes that snapshot over a plain TCP port to any number of consumers, in any language.
Consumers keep their own in-memory copy and never touch the network or the database on their hot path.

The design is in `docs/DESIGN.md` (with diagrams); the decisions taken while implementing it are in
`docs/IMPLEMENTATION-NOTES.md`. This version covers the server, the database loading, the chunked protobuf
publication over TCP with framing, timeouts and limits, a Java client library and a Python sample client.
Live updates are out of scope.

## What is different from the design (environment constraints)
* **No Hibernate**: this machine cannot reach Maven Central, so the loader is plain JDBC (`refdata-loader-jdbc`)
  behind the `RefDataSource` interface the design defined for this swap. Nothing downstream knows about JDBC.
* **Apache Derby (embedded)** instead of PostgreSQL, standard SQL only. The same reason.
* All third-party jars are checked in under `lib/` (provenance in `lib/README.md`); the build runs offline.

## Layout
| Module | Contents |
|---|---|
| `refdata-model` | records, enums, indexes, `RefDataStore` (+ builder and validation), `RefDataSource` interface |
| `refdata-contract` | `protocol.proto`, `entities.proto`, generated classes, framing constants, digests, model/proto codec |
| `refdata-db` | Derby schema, embedded database helper, deterministic data generator (`gradle :refdata-db:dataGen`) |
| `refdata-loader-jdbc` | `JdbcRefDataSource` |
| `refdata-server` | configuration, snapshot encoder, Netty TCP server, health/metrics HTTP, `RefDataServerMain` |
| `refdata-client` | Java consumer library (`RefDataClient`) and a demo |
| `refdata-integration-tests` | end-to-end tests: database → server → client, fault injection |
| `tools/python-client` | a ~200-line Python consumer proving the protocol is language-independent |
| `docs/` | design document, diagrams, the two .proto files, implementation notes |

## Build and test
Requirements: Java 21 and Gradle 8 (both installed here; `gradle` is on the PATH). Everything runs offline.

    gradle build            # compiles everything and runs the unit tests (~170)
    gradle integrationTest  # the slow end-to-end tests (database, TCP, fault injection)

## Generate data
    gradle :refdata-db:dataGen -Pargs="--db data/tiny --profile TINY"
    gradle :refdata-db:dataGen -Pargs="--db data/medium --profile MEDIUM"     # 100k clients, 1.13M rows, ~70 s

Profiles: TINY (1k clients), MEDIUM (100k), LARGE (1M, ≈11M rows), XLARGE (5M). Same seed ⇒ same rows.

## Run the service
    gradle :refdata-server:run --args="config/refdata-server.properties"

or build a distribution: `gradle :refdata-server:installDist` then
`refdata-server/build/install/refdata-server/bin/refdata-server config/refdata-server.properties`.
Any setting can be overridden with `-Drds.key=value`. Then:

    curl localhost:7401/health/ready      # 200 once the snapshot is published (503 + phase before)
    curl localhost:7401/snapshot          # id, data version, counts, hashes
    curl localhost:7401/metrics           # Prometheus-style text

## Consume the snapshot
Java:

    RefDataClient rds = RefDataClient.builder().endpoints("localhost:7400").clientName("demo").build();
    LoadedSnapshot snap = rds.load();                 // handshake, chunks, checks, indexes, atomic publish
    RefDataStore data = snap.store();
    Account a = data.accountByNumber("GB12...");      // allocation-free lookups, null when absent

Demo: `java -cp ... com.example.refdata.client.RefDataClientDemo localhost:7400` (loads, prints counts, times 1M lookups).

Python: `tools/python-client/run.sh --host localhost --port 7400` (see its README).

## Configuration

### Optional cache inspection UI

Server inspection is disabled by default. Add these properties to the server configuration:

```properties
rds.inspection.enabled=true
rds.inspection.host=127.0.0.1
rds.inspection.port=7402
rds.inspection.max.page.size=100
```

Open [the local inspector](http://127.0.0.1:7402/). The host defaults to loopback independently
of the TCP/health bind address. The port defaults to 7402; port 0 selects a free port
(`server.inspectionPort()`). The UI shows all six cache names and counts, snapshot ID,
data/update version, records in primary-key order, Previous/Next links, and primary-key lookup
(country/currency code, numeric ID for the other caches). An empty key returns to paging.

Client applications opt in on the builder, choosing a port per application:

```java
import com.example.refdata.model.inspection.InspectionServer;

try (RefDataClient client = RefDataClient.builder()
        .endpoints("localhost:7400").clientName("demo")
        .inspection(new InspectionServer.Options("127.0.0.1", 7502, 100))
        .build()) {
    LoadedSnapshot snapshot = client.load();
    // Browse http://127.0.0.1:7502/ while the application runs.
}
```

Inspection returns 503 before the first successful client load, follows subsequent successful
loads, and retains the last successful view if a load fails. Closing the client/server closes
the inspection listener and executor. Disabled inspection opens no port or executor.

Each request captures one immutable store/version pair. Inspection uses indexed lookups and
read-only list views without copying or scanning the entire cache, taking publication locks,
or sharing application reader/update executors. Pages default to 50 rows, with a configurable
maximum of 1–500 (default 100); invalid limits/offsets return 400. Two dedicated daemon workers,
a 16-request queue and a listen backlog of 16 bound inspection concurrency; overload rejects
connections rather than running inspection on the HTTP dispatcher or application threads.
Only GET is accepted, responses are not cached, and values are HTML-escaped. No new dependencies
are required: this uses the existing JDK HTTP server. The inspector has no authentication;
keep the default loopback host unless access is controlled externally.

This checkout publishes a startup snapshot on the server and supports repeated client loads;
it does not implement live delta updates. Pages capture the latest publication independently,
so check the displayed snapshot/version when paging across a reload (no historical stores are retained).

See `config/refdata-server.properties` for the common keys and `docs/DESIGN.md` section 16 for all of them.
Logging: `config/log4j2.xml`.
