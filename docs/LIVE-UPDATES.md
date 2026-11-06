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
- A shared immutable journal retains 32 batches (at most about 16 MiB of payload).
  Each publication is pushed to live sessions at once (with a 50 ms re-check as a fallback) after
  snapshot completion; no unbounded per-client queue.
- Slow consumers are disconnected or forced to reload when the retained history is insufficient.
- The outbox is drained every 100 ms and immediately after admin commits; old applied
  entries are pruned. Committed rows and release versions survive process restart.
- This version uses dataVersion as the monotonic transaction version. Each transaction
  increments it by one. Database release IDs must follow the same convention.
- Wire delivery is incremental. Existing array-based stores/indexes are rebuilt off the
  reader path per transaction, and a replacement encoded snapshot is prepared for future
  connections. This favours correctness for reference-data workloads; it is not yet a
  high-frequency persistent-index implementation. Batch related edits to amortise rebuilds.
- Direct SQL writes outside this API are not automatically detected. Stop the writer and
  restart/reload the service after such maintenance. A coordinated online full-reload
  operation and external database CDC remain separate roadmap work.

## Verification

LiveUpdatesIT covers database persistence and restart, two simultaneous consumers,
all-entity CRUD, rollback, an update committed during an active TCP snapshot transfer, and
resume with replay of updates committed while a consumer was disconnected.
Run gradle test integrationTest. On Windows, generate protocol sources with the bundled
Linux protoc under WSL first, then skip :refdata-contract:generateProto for the Gradle run.
