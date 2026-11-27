# Incremental live updates

## Supported flow

The service-owned admin API commits entity mutations, the release version and a binary
outbox payload in one Derby transaction. The service drains committed outbox entries,
builds a validated immutable store and publishes the change to opted-in TCP consumers.
The inspector reads the latest immutable store.

Each consumer receives its initial full snapshot first. Incremental transmission is gated
on successful completion of the SnapshotEnd write, on the same ordered TCP connection.
Clients verify base versions and atomically replace their local immutable store per batch.
Previously returned stores remain unchanged.

## Enable the admin writer

Add these settings to the server properties file:

```properties
rds.admin.port=7403
rds.admin.token=replace-with-a-private-token-at-least-16-characters
```

The admin listener binds only to 127.0.0.1. A negative port (default -1) disables it;
port 0 chooses a free port exposed by server.adminPort(). The existing rds.db.url,
rds.db.user and rds.db.password settings select the database. The writer creates
REFDATA_OUTBOX automatically in that database. Use one reference-data writer service
per embedded database. Start administration only after the service is ready.

POST /changes accepts ChangeCodec.encode(ChangeBatch) as its binary body and requires
Authorization: Bearer <token>. Responses: 204 committed and published; 400 malformed body;
401 unauthorized; 409 invalid mutation/version or a result that fails validation;
413 oversized body; 503 database/publication failure. Every non-2xx outcome is logged by
`com.example.refdata.server.admin.AdminServer` (implementation: `AdminServer` for HTTP,
`ChangeWriter` for the transactional write, `OutboxRelay` for publication).
After an ambiguous response, inspect the current version before retrying. A stale base
version is rejected, so blindly replaying a committed transaction does not apply it twice.
The endpoint is deliberately not an arbitrary SQL execution API.

## Full reload after direct SQL fixes

Changes made with SQL outside `POST /changes` are not detected. After such maintenance, call:

```
POST /reload
Authorization: Bearer <token>
```

The service re-reads the whole database with the startup rules (connection retries, sanity minimums,
validation policy, heap check), encodes a replacement snapshot off the reader path, and only then cuts over.
Readers, inspection and transfers already in progress keep the previous data until the cutover. The cutover
publishes the new snapshot as a **new update generation**: the retained update journal is dropped, new
connections get the new snapshot, and every live session of the previous generation is closed with fatal
`SNAPSHOT_NOT_AVAILABLE`. The client's resume is refused (other generation) and it loads the new snapshot;
the last valid store stays readable meanwhile. Snapshot-only clients get the new data on their next load.

Coordination: `/changes` commits and the outbox relay are held back for the duration. Pending outbox batches
are published first when they can be; their rows are committed either way, so the reload includes them. No
committed batch is lost or applied twice. Admin batches after the reload use the reloaded data version as
their base.

Data version: the reload serves the database's latest `PUBLISHED` release. Add a release row with the next
version for a SQL fix when you can, so versions stay meaningful in logs; a fix without one is still published
(same data version, new snapshot id and generation). A database **behind** the served version is refused,
because publishing it would make the relay re-apply batches already in the data.

Responses: 200 published, with a JSON body
`{"previousDataVersion":7,"dataVersion":8,"snapshotId":"…","rows":{"COUNTRY":250,…},"loadMs":…,"totalMs":…}`;
401 unauthorized; 405 not POST; 409 refused (another reload running, or the database is behind the served
version); 422 the database content fails validation; 501 the service was started without a database;
503 the database could not be read. On every non-200 outcome nothing changes and the previous data stays served.
`RefDataServer.reload()` does the same in-process.

Sizing: until the cutover both the previous and the replacement data are in memory, so the heap must hold two
copies. Every live consumer reloads a full snapshot after the cutover; the transfer slots and queue
(`rds.max.concurrent.snapshots`, `rds.snapshot.queue.size`) pace that burst. A long reload delays `/changes`
requests queued behind it (one admin worker, 16-request queue).

## Admin Java example

```java
Country changed = new Country("GB", "GBR", 826, "Updated name", "Europe");
ChangeBatch batch = new ChangeBatch(currentVersion, currentVersion + 1,
    List.of(new ChangeBatch.Change(EntityKind.COUNTRY,
        ChangeBatch.Operation.UPDATE, "GB", changed)));
HttpRequest request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:7403/changes"))
    .header("Authorization", "Bearer " + token)
    .header("Content-Type", "application/octet-stream")
    .POST(HttpRequest.BodyPublishers.ofByteArray(ChangeCodec.encode(batch)))
    .build();
```

ChangeBatch is in com.example.refdata.model; ChangeCodec is in com.example.refdata.contract.
The same API supports INSERT, UPDATE and DELETE for all six entity types. Deletes carry
null instead of a record. Country/currency primary keys are codes; other keys are decimal
IDs. Updates carry complete after-images. Coalesce multiple mutations of the same key
within one transaction. Inserts reject existing keys; updates/deletes reject absent keys.
The final graph must pass model validation. Supply database operations in foreign-key
order: insert parents before children and delete children before parents. Invalid SQL
ordering or constraints roll back the entire database transaction.

## Live Java consumers

```java
try (RefDataClient client = RefDataClient.builder()
        .endpoints("localhost:7400").clientName("orders")
        .liveUpdates(true).build()) {
    client.load(); // initial snapshot has been verified and installed
    RefDataStore view = client.currentStore();
    // Obtain currentStore() for each unit of work; retain one view for consistent related reads.
}
```

Live mode requires all entity types and keep-alive, which liveUpdates(true) enables.
Start one subscription per client instance. Older servers are rejected explicitly in live
mode; snapshot-only clients retain their existing wire behaviour. On connection loss,
live consumers reconnect and resume from their last applied data version: the server replays
the retained batches after it (`SnapshotRequest.live_resume` -> `UpdatesResumed`). After a
server restart (new `HelloAck.update_generation`), expired history or an invalid update, they
load a fresh full snapshot instead. Duplicate batches are skipped.
The last valid store remains readable during recovery. Closing the client stops its subscription.
Snapshot IDs shown by client inspection identify the initial snapshot of the connection;
the store data/update version advances with each applied batch.

## Limits and implementation tradeoffs

- A batch contains at most 1,000 mutations and 512 KiB of encoded payload, and must fit the
  configured TCP frame limit. The admin executor has one worker and a 16-request queue.
- A shared immutable journal retains the newest `rds.live.history.batches` (default 32) batches and at most
  `rds.live.history.bytes` (default 16 MiB, at least `rds.frame.max.bytes`) of encoded updates; the newest
  batch is always kept. It is shared by all sessions, so memory does not grow with the number of consumers.
  Each publication is pushed to live sessions at once (with a 50 ms re-check as a fallback) after snapshot
  completion; there is no per-client queue.

## Slow consumers and resource bounds

Per live session, the server writes updates only while the connection is below its write-buffer high-water mark
(`rds.write.buffer.high.water`); everything not yet written stays in the shared journal. A consumer that stops
reading therefore costs at most one write buffer, and publication never waits for it.

- **Brief lag is tolerated.** When updates are due but the connection is not writable, the session starts a lag
  clock; delivery continues as soon as the connection drains (write-buffer low-water mark), and the clock resets
  once the session has caught up.
- **Sustained lag is cut off.** A session that has had updates due without accepting them for `rds.live.max.lag`
  (default 30 s) is closed with fatal `SLOW_CONSUMER`. The client reconnects and resumes from its last applied
  version when the journal still covers it.
- **Falling behind the journal forces a reload.** If the next update a session needs has left the journal, it is
  closed with fatal `SNAPSHOT_NOT_AVAILABLE`, whether or not it is currently reading; the client's resume is
  refused and it loads a fresh snapshot. The last valid store stays readable throughout.
- **Shutdown** sends GoAway to live sessions at once; the drain phase waits only for running snapshot transfers,
  never for a lagging live consumer.
- Snapshot transfers keep their own bounds: `rds.stall.timeout` (no write progress) and `rds.snapshot.max.duration`.

Metrics on `/metrics`: `rds_live_sessions_active`, `rds_live_sessions_lagging`, `rds_live_history_batches`,
`rds_live_history_bytes`, `rds_live_updates_sent_total`, `rds_live_resyncs_total{reason="slow_consumer"|"history_expired"|"reload"}`,
`rds_reloads_total{outcome="published"|"rejected"|"failed"}` and `rds_last_reload_duration_ms`.

Sizing: the journal should cover the longest disconnection you want consumers to survive without a reload
(publication rate x outage); `rds.live.max.lag` should exceed the longest pause a healthy consumer may take
(for example a GC pause), and stay well below the time the journal covers.

Batch bounds are unchanged: at most 1,000 mutations and 512 KiB per batch; the admin executor has one worker
and a 16-request queue.
- The outbox is drained every 100 ms and immediately after admin commits; old applied
  entries are pruned. Committed rows and release versions survive process restart.
- This version uses dataVersion as the monotonic transaction version. Each transaction
  increments it by one. Database release IDs must follow the same convention.
- Wire delivery is incremental. Existing array-based stores/indexes are rebuilt off the
  reader path per transaction, and a replacement encoded snapshot is prepared for future
  connections. This favours correctness for reference-data workloads; it is not yet a
  high-frequency persistent-index implementation. Batch related edits to amortise rebuilds.
- Direct SQL writes outside this API are not automatically detected. Run `POST /reload` after
  such maintenance (see above). External database CDC remains separate roadmap work.

## Verification

LiveUpdatesIT covers database persistence and restart, two simultaneous consumers,
all-entity CRUD, rollback, an update committed during an active TCP snapshot transfer, and
resume with replay of updates committed while a consumer was disconnected, and a consumer away longer
than the retained history reloading a snapshot. `SessionHandlerLiveDeliveryTest`, `LiveHistoryTest` and
`SlowLiveConsumerTest` (refdata-server) cover lag tolerance, `SLOW_CONSUMER` disconnection, journal bounds,
a stalled consumer next to a healthy one under a burst of ~19 MB of updates, and shutdown with a stalled consumer.
Full reload: `RefDataServerReloadTest` (cutover, new generation, live session resync, same-version fix, refusal of a
database behind the served version, invalid content, unreachable database, no database), `LiveHistoryTest` and
`SessionHandlerLiveResumeTest` (journal reset, sessions of the previous generation), and `LiveUpdatesIT` (direct SQL
fix published by `POST /reload` to a live consumer, then admin batches on the new generation; 401/409/422).
Run gradle test integrationTest. On Windows, generate protocol sources with the bundled
Linux protoc under WSL first, then skip :refdata-contract:generateProto for the Gradle run.
