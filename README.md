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
See `config/refdata-server.properties` for the common keys and `docs/DESIGN.md` section 16 for all of them.
Logging: `config/log4j2.xml`.
