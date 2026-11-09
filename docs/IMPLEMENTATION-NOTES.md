# Implementation notes (version 0.1 of the code)

Decisions taken while implementing the design (DESIGN.md). Where this file and DESIGN.md differ, this file wins.

## Environment substitutions
* **No Hibernate** (Maven Central unreachable from this desktop): the loader is plain JDBC (`refdata-loader-jdbc`)
  behind the `RefDataSource` interface in `refdata-model`.
* **Apache Derby (embedded)** instead of PostgreSQL. Standard SQL only. Consistent read: one transaction per table at
  REPEATABLE READ; the data-version re-check after the load guards the "static data" assumption.
* All jars are in `lib/` (see lib/README.md); Gradle runs offline; `tools/protoc` generates the Java contract.

## Module map
| Module | Contents |
|---|---|
| refdata-model | records, enums (with UNKNOWN), indexes (LongIndex, AsciiIndex, CsrIndex), RefDataStore(+Builder, validation), RefDataSource/RefDataSink, testing.IntegrationTest marker |
| refdata-contract | protocol.proto, entities.proto, generated classes (packages com.example.refdata.protocol.v1 / .schema.v1, one top-level class per message), Framing constants, Digests, EntityCodec (model <-> proto) |
| refdata-db | Derby schema DDL, embedded DB helper, deterministic data generator + CLI |
| refdata-loader-jdbc | JdbcRefDataSource |
| refdata-server | config, snapshot encoder/store, Netty TCP server, health HTTP, main |
| refdata-client | Java consumer library |

## Wire behaviour both sides implement (precise form of DESIGN.md section 9)
* Frame: 4-byte big-endian length (excludes itself) + one `Envelope`. Server inbound limit 64 KiB (`Framing.SERVER_INBOUND_MAX_FRAME_BYTES`); server never sends a frame larger than `rds.frame.max.bytes` (default 1 MiB); `Hello.max_frame_bytes == 0` means 1 MiB.
* Handshake: client sends Hello first. Server checks `protocol_major == Framing.PROTOCOL_MAJOR` (0 or other → UNSUPPORTED_PROTOCOL_VERSION, fatal), a common schema major (→ else UNSUPPORTED_SCHEMA_VERSION, fatal), client max frame ≥ largest server frame (→ else FRAME_TOO_LARGE, fatal). Codec = first of `Hello.accepted_codecs` that the server has pre-encoded, else NONE. HelloAck always carries `heartbeat_interval_ms` and `current_snapshot`.
* Chunk payload = serialized `<Entity>Batch` (entities.proto), optionally compressed: LZ4 = lz4-java block format (`LZ4Factory.fastestInstance()`, decompress with the known `uncompressed_size`), ZSTD = zstd-jni (`Zstd.compress` / `Zstd.decompressByteArray`). `crc32` = zlib CRC-32 (`Digests.crc32`) over the payload bytes as sent (i.e. compressed when compressed).
* Hashes: `entity_sha256` = SHA-256 over the *uncompressed* payloads of that entity type in seq order; `content_sha256` = `Digests.contentSha256(list of entity hashes in manifest order)`; `snapshot_id` = `Digests.snapshotId(schemaMajor, schemaMinor, chunkEntityTypes, chunkRecordCounts, contentSha256)`.
* Chunking: one entity type per chunk, primary-key order, types in `EntityKind` order; target `rds.chunk.target.bytes` (default 256 KiB) of uncompressed payload; a chunk is closed before a record would push the *frame* over `rds.frame.max.bytes`; startup fails if `chunk.target + Framing.CHUNK_HEADROOM_BYTES > frame.max`. `seq` counts all chunks of the full snapshot from 0; the manifest gives `first_seq`/`chunk_count` per type.
* Request: `entity_types` empty = all; unknown numbers ignored; COUNTRY and CURRENCY are always included by the client library when it requests a subset. `request_id >= 1`. Resume = non-empty `resume_snapshot_id` + `resume_from_seq` (must be inside the requested ranges, or exactly one past the last requested chunk — then the server answers SnapshotBegin{first_seq} + SnapshotEnd with the full totals and no chunks; the filter must equal the original). `if_snapshot_id` equal to the current id → SnapshotBegin{not_modified=true} then SnapshotEnd{0 chunks}; never queued.
* Session states: AWAIT_HELLO → READY → (QUEUED →) STREAMING → READY. Out-of-order message (anything before Hello, second Hello) → PROTOCOL_VIOLATION fatal. Second SnapshotRequest while queued/streaming → INVALID_REQUEST non-fatal. Envelope with no known body → UNSUPPORTED_MESSAGE non-fatal. Client may send ErrorReply{INTEGRITY_FAILURE, seq} in any state and then closes; the server re-checks that frame's CRC and logs.
* Timeouts (server): handshake 10 s (HANDSHAKE_TIMEOUT), READY idle 60 s without keep-alive (IDLE_TIMEOUT), stall 30 s of no write progress while streaming (close, SLOW_CONSUMER best effort), max transfer duration 10 min, heartbeat interval 5 s, 3 misses = dead. Limits: max connections 256 (SERVER_BUSY sent before closing), max concurrent snapshots 8, queue 64 / wait 60 s (SnapshotQueued, then SERVER_BUSY with retry_after_ms). Live updates: shared journal of 32 batches / 16 MiB (`rds.live.history.*`); updates written only while the channel is writable; updates due but not accepted for `rds.live.max.lag` (30 s) → SLOW_CONSUMER fatal (client resumes); next needed update gone from the journal → SNAPSHOT_NOT_AVAILABLE fatal (client reloads).
* Heartbeats: server → client while QUEUED and on kept-alive connections; client → server only on kept-alive connections. Without keep-alive the client closes after SnapshotEnd.
* Shutdown: GoAway{SERVER_SHUTTING_DOWN, reconnect_after_ms} to all sessions, stop accepting, drain up to 30 s, close.
* Client checks (9.7): frame size; seq == expected; CRC; decoded record count == record_count; per-entity SHA-256 and count when a type completes; totals vs manifest and SnapshotEnd; whole hash only for unfiltered loads. Chunks of unknown entity types: seq/CRC checked, contents skipped. Only then build indexes and publish atomically.

## Conventions
* Java 21, records, `final` classes, slf4j logging, no dependency outside lib/.
* Unit tests: JUnit 4 in each module (`gradle test`). Slow end-to-end tests: `@Category(com.example.refdata.testing.IntegrationTest.class)` (`gradle integrationTest`).
* Configuration keys and defaults exactly as DESIGN.md section 16 (`rds.*`), read from a properties file with system-property overrides.
