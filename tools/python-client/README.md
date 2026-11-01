# Python sample client

A small consumer of the reference data service written in Python, to show that the wire contract
(`refdata-contract/src/main/proto`) is language independent. It uses only the standard library and the
`protobuf` runtime, speaks the protocol directly (4-byte big-endian length prefix + `Envelope`), and performs all
consumer-side integrity checks of design section 9.7: chunk sequence numbers, CRC-32 per chunk, record count and
uncompressed size per chunk, per-entity SHA-256, whole-snapshot hash over the entity hashes (and against
`SnapshotEnd` for unfiltered loads), and the `SnapshotEnd` totals. Exit code 0 only when every check passed.

## Files

* `refdata_client.py` – the client.
* `run.sh` – regenerates the protobuf classes into `gen/` with `tools/protoc` and runs the client (no Gradle needed).
* `gen/` – generated `*_pb2.py` modules (created by `run.sh`; safe to delete).

## Requirements

* Python 3 with the `protobuf` package: `python3 -c "import google.protobuf"` must succeed (on this machine it is
  installed system-wide, version 4.25; otherwise `pip install protobuf`, or set `PYTHONPATH` to a directory that
  has it, e.g. `/tmp/rds/gt`).
* Optional: `lz4` and `zstandard` packages if the server is configured to offer those codecs. The client only
  announces `CODEC_NONE` in its Hello, so by default the server sends uncompressed chunks and neither is needed.

## Generate and run

```
tools/python-client/run.sh                           # all entity types from localhost:7400
tools/python-client/run.sh --host h --port 7400      # another server
tools/python-client/run.sh --types CLIENT ACCOUNT    # a subset (exactly the types named; the Java library would add COUNTRY and CURRENCY)
```

Generating by hand:

```
tools/protoc -I refdata-contract/src/main/proto --python_out=tools/python-client/gen \
    refdata-contract/src/main/proto/refdata/protocol/v1/protocol.proto \
    refdata-contract/src/main/proto/refdata/schema/v1/entities.proto
python3 tools/python-client/refdata_client.py [--host H] [--port P] [--types T ...] [--timeout S]
```

## Starting a server to talk to

```
gradle :refdata-server:installDist
printf 'rds.db.url=jdbc:derby:%s/data/tiny\nrds.tcp.port=7400\nrds.http.port=7401\n' "$PWD" > /tmp/rds.properties
refdata-server/build/install/refdata-server/bin/refdata-server /tmp/rds.properties
curl -sf localhost:7401/health/ready     # READY once the snapshot is published
```

## Output

The client prints the negotiated versions and codec, the snapshot id and data version, a table of records and
chunks per entity, bytes and time, one sample record per entity type and the number of integrity checks passed.
