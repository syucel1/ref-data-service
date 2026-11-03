# Reference Data Service · Roadmap

> Planned · 15 milestones · Small, reviewable commits

A path from snapshot delivery to reliable incremental updates, safe reloads and a clearer inspection experience. All items are planned; this document does not imply implementation is complete.

[Open the visual roadmap](roadmap.html)

## At a glance

| Phase | Milestones | Outcome |
|---|---|---|
| 01 · Foundations | 1–3 | Agreed consistency rules, publication versions and committed change tracking |
| 02 · Server updates | 4–6 | Validated immutable CRUD batches and an update protocol |
| 03 · Consumer delivery | 7–10 | Ordered delivery, atomic client updates, replay and bounded queues |
| 04 · Operations | 11–14 | Safe reloads, protected controls, recovery tests and monitoring |
| 05 · Inspection | 15 | Readable tables, clear pagination and visible publication versions |

**Start here:** milestone 1 is a documentation change defining update and consistency rules.
Milestone 2 is the first implementation change: publication versions.

## Agreed direction

- Use a transactional outbox for admin-driven database changes.
- Use an explicit full reload for direct SQL fixes; automatic change capture is a later extension.
- Make each committed transaction/batch visible atomically.
- Send the complete snapshot before incremental updates on each consumer connection.
- Preserve immutable, non-blocking reader views and bound queues, history and inspection work.

## How to use this roadmap

Work in numbered order by default. Each milestone suggests two initial commit slices; split further when needed. Add meaningful tests with each behaviour change, rather than waiting for milestone 13. Mark a milestone complete only when implementation, tests and documentation are ready. Milestone 12 is a release gate for externally accessible administrative controls.

## 01 · Foundations

Agree on consistency before changing publication.

### 1. Define update and consistency rules

- [ ] Complete

Specify insert, update and delete behaviour for every entity, transaction boundaries, ordering, relationship validation and deletion of referenced records.

Suggested commits:

1. Document CRUD and foreign-key rules.
2. Document atomic batch visibility and failure behaviour.

### 2. Introduce publication versions

- [ ] Complete

Identify each published state with a snapshot generation and monotonic update sequence. Include the expected base version so consumers can detect gaps.

Suggested commits:

1. Add publication version types and invariants.
2. Expose versions in snapshot metadata and tests.

### 3. Track committed database changes

- [ ] Complete

Use a transactional outbox/change log for admin-driven changes, written in the same database transaction. Direct SQL fixes initially require an explicit full reload.

Suggested commits:

1. Add outbox schema and transaction tests.
2. Add bounded polling and checkpoint handling.

## 02 · Server updates

Build and publish complete, valid immutable views.

### 4. Reload and validate changed records

- [ ] Complete

Read committed changes and reload affected records. Preserve database transaction boundaries and validate related records as one batch.

Suggested commits:

1. Read and group committed change batches.
2. Validate batches and define failure/retry handling.

### 5. Apply CRUD batches to immutable server views

- [ ] Complete

Implement all operations across all six entity types, rebuild affected indexes and atomically publish the next view. Readers and inspection must not block publication.

Suggested commits:

1. Implement batch application and relationship tests.
2. Publish immutable views and test concurrent readers.

### 6. Extend the incremental update protocol

- [ ] Complete

Define operation types, deletion keys, transaction batches, sequence numbers and compatibility negotiation. Support snapshot-only clients where negotiation permits.

Suggested commits:

1. Add wire messages and codec tests.
2. Negotiate update support and document compatibility.

## 03 · Reliable consumer delivery

Preserve ordering, recover from interruptions and bound resource use.

### 7. Guarantee snapshot-before-update delivery

- [ ] Complete

Capture a snapshot watermark. Deliver the full snapshot before later update batches on each connection, with bounded buffering and an explicit resync path.

Suggested commits:

1. Add per-session snapshot/update state transitions.
2. Test updates arriving during snapshot transfer.

### 8. Apply client updates atomically

- [ ] Complete

Validate and apply each transaction batch to a new immutable client view. Publish the whole batch at once and preserve the previous view on failure.

Suggested commits:

1. Implement client batch application.
2. Test atomic visibility and failed-batch recovery.

### 9. Reconnect, replay and resynchronise

- [ ] Complete

Reconnect with the last applied sequence. Replay retained updates when possible; fall back to a fresh snapshot after gaps, incompatible generations or expired history.

Suggested commits:

1. Implement bounded replay history and resume negotiation.
2. Add duplicate, gap and restart recovery tests.

### 10. Bound delivery and handle slow consumers

- [ ] Complete

Limit queue sizes, retained history, batch sizes and processing time. Disconnect and resynchronise lagging consumers without delaying other consumers or publication.

Suggested commits:

1. Enforce budgets and overload behaviour.
2. Test slow consumers, saturation and shutdown.

## 04 · Operations and confidence

Make reloads safe, controls accountable and failures observable.

### 11. Add an explicit full-reload operation

- [ ] Complete

Reload and validate the database after direct SQL fixes, publish a new snapshot generation and propagate it to consumers. Coordinate concurrent updates so changes are neither lost nor applied twice.

Suggested commits:

1. Build and validate replacement snapshots off the reader path.
2. Coordinate reload cutover, update tracking and consumer resync.

### 12. Secure and audit administrative operations

- [ ] Complete

Protect reload/control endpoints, audit requests and expose progress and failures. Keep inspection read-only and separate from administration. Security must be ready before exposing reload controls beyond local development.

Suggested commits:

1. Add operation status and audit records.
2. Protect administrative access and test rejection paths.

### 13. Expand integration and recovery coverage

- [ ] Complete

Add tests alongside every milestone, then complete a cross-cutting failure matrix: all entities and CRUD operations, rollback, related records, reload races, snapshot overlap, duplicate/missing updates, reconnect and shutdown.

Suggested commits:

1. Add the CRUD and transaction integration matrix.
2. Add fault injection, resource-bound and recovery scenarios.

### 14. Expand Prometheus metrics and add Grafana dashboards

- [ ] Complete

Extend the existing metrics endpoint with update throughput, publication versions, consumer lag, queue utilisation, replay/resync counts, reload duration and failures.

Suggested commits:

1. Add metric definitions and verification.
2. Add provisionable Grafana dashboards and alert examples.

## 05 · Inspection experience

Make large caches easy to explore without increasing application load.

### 15. Modernise the cache inspection UI

- [ ] Complete

Replace long record lines with readable tables, clear columns, cache counts, current/total page numbers, Previous/Next controls, key lookup and an explicit refresh action. Keep snapshot/update versions visible and all requests bounded.

Suggested commits:

1. Add responsive table layout, navigation and empty/error states.
2. Add page counts, refresh behaviour and browser verification.

## Done means

- Behaviour and recovery rules are documented.
- Relevant unit and integration checks pass, including concurrency and resource limits.
- Protocol compatibility and deployment implications have been reviewed.
- Metrics and operational guidance cover the new behaviour where applicable.

## Later candidates

Consider automatic database change capture, high-availability publisher failover, selective subscriptions, and performance benchmarking after the core update/recovery path is established. These are not part of the 15 milestones above.
