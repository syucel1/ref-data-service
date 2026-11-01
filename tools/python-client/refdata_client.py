#!/usr/bin/env python3
"""Sample non-Java consumer of the reference data service (design section 14.2).

Speaks the wire protocol directly: 4-byte big-endian length-prefixed protobuf Envelopes, Hello/HelloAck,
SnapshotRequest and the chunk stream, with every consumer-side integrity check of design section 9.7
(sequence numbers, CRC-32 per chunk, record counts, per-entity SHA-256, whole-snapshot hash, SnapshotEnd totals).
Exit code 0 only if every check passed. Needs the generated classes in ./gen (see run.sh) and the 'protobuf' package.
"""
import argparse
import hashlib
import os
import socket
import struct
import sys
import time
import zlib

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "gen"))
from refdata.protocol.v1 import protocol_pb2 as P  # noqa: E402
from refdata.schema.v1 import entities_pb2 as S  # noqa: E402

PROTOCOL_MAJOR = 1
SCHEMA_MAJORS = [1]
MAX_FRAME = 1 << 20
BATCH_BY_TYPE = {
    S.ENTITY_TYPE_COUNTRY: ("COUNTRY", S.CountryBatch),
    S.ENTITY_TYPE_CURRENCY: ("CURRENCY", S.CurrencyBatch),
    S.ENTITY_TYPE_CLIENT: ("CLIENT", S.ClientBatch),
    S.ENTITY_TYPE_ADDRESS: ("ADDRESS", S.AddressBatch),
    S.ENTITY_TYPE_ACCOUNT: ("ACCOUNT", S.AccountBatch),
    S.ENTITY_TYPE_SETTLEMENT_INSTRUCTION: ("SETTLEMENT_INSTRUCTION", S.SettlementInstructionBatch),
}
TYPE_BY_NAME = {name: t for t, (name, _) in BATCH_BY_TYPE.items()}


class ProtocolError(Exception):
    pass


class Connection:
    """Length-prefixed Envelope framing over a blocking socket."""

    def __init__(self, host, port, timeout):
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.bytes_in = 0

    def send(self, envelope):
        data = envelope.SerializeToString()
        self.sock.sendall(struct.pack(">I", len(data)) + data)

    def recv(self):
        length = struct.unpack(">I", self._read(4))[0]
        if length == 0 or length > MAX_FRAME:
            raise ProtocolError("frame length %d outside 1..%d" % (length, MAX_FRAME))
        env = P.Envelope()
        env.ParseFromString(self._read(length))
        self.bytes_in += 4 + length
        return env

    def _read(self, n):
        buf = bytearray()
        while len(buf) < n:
            part = self.sock.recv(n - len(buf))
            if not part:
                raise ProtocolError("connection closed by server")
            buf.extend(part)
        return bytes(buf)

    def close(self):
        self.sock.close()


def decompress(chunk):
    if chunk.codec in (P.CODEC_UNSPECIFIED, P.CODEC_NONE):
        return chunk.payload
    if chunk.codec == P.CODEC_LZ4:
        import lz4.block  # optional dependency
        return lz4.block.decompress(chunk.payload, uncompressed_size=chunk.uncompressed_size)
    if chunk.codec == P.CODEC_ZSTD:
        import zstandard  # optional dependency
        return zstandard.ZstdDecompressor().decompress(chunk.payload, max_output_size=chunk.uncompressed_size)
    raise ProtocolError("unknown codec %d" % chunk.codec)


def error_text(err):
    return "%s (fatal=%s): %s" % (P.ErrorCode.Name(err.code), err.fatal, err.message)


def handshake(conn):
    hello = P.Hello(protocol_major=PROTOCOL_MAJOR, protocol_minor=0, schema_majors=SCHEMA_MAJORS, max_frame_bytes=MAX_FRAME,
                    client_name="python-sample", client_instance="%s/%d" % (socket.gethostname(), os.getpid()),
                    library_version="refdata_client.py 0.1", accepted_codecs=[P.CODEC_NONE])
    conn.send(P.Envelope(hello=hello))
    env = conn.recv()
    if env.HasField("error"):
        raise ProtocolError("handshake rejected: " + error_text(env.error))
    if not env.HasField("hello_ack"):
        raise ProtocolError("expected HelloAck, got " + (env.WhichOneof("body") or "empty envelope"))
    return env.hello_ack


def run(args):
    checks = []  # (description, passed)

    def check(desc, ok):
        checks.append((desc, ok))
        if not ok:
            print("CHECK FAILED: " + desc)

    conn = Connection(args.host, args.port, args.timeout)
    started = time.time()
    ack = handshake(conn)
    print("connected to %s:%d: server %s %s, protocol %d.%d, schema %d.%d, codec %s, session %s" % (
        args.host, args.port, ack.server_id, ack.server_version, ack.protocol_major, ack.protocol_minor,
        ack.schema_major, ack.schema_minor, P.Codec.Name(ack.codec), ack.session_id))
    check("HelloAck max_frame_bytes <= ours", ack.max_frame_bytes <= MAX_FRAME)

    wanted = [TYPE_BY_NAME[t] for t in args.types] if args.types else []
    conn.send(P.Envelope(snapshot_request=P.SnapshotRequest(request_id=1, entity_types=wanted)))

    begin = None
    while begin is None:
        env = conn.recv()
        kind = env.WhichOneof("body")
        if kind == "snapshot_begin":
            begin = env.snapshot_begin
        elif kind == "snapshot_queued":
            print("queued: position %d, max wait %d ms" % (env.snapshot_queued.queue_position, env.snapshot_queued.max_wait_ms))
        elif kind == "heartbeat":
            pass
        elif kind == "error":
            raise ProtocolError("request rejected: " + error_text(env.error))
        elif kind == "go_away":
            raise ProtocolError("server going away: " + env.go_away.message)
        else:
            raise ProtocolError("unexpected message before SnapshotBegin: %s" % kind)

    info = begin.snapshot
    manifests = {m.entity_type: m for m in info.entities}
    sent_types = list(begin.entity_types)
    print("snapshot %s, data version %d, %d chunks / %d records / %d bytes in total; receiving %s" % (
        info.snapshot_id.hex(), info.data_version, info.total_chunks, info.total_records, info.total_uncompressed_bytes,
        ", ".join(BATCH_BY_TYPE.get(t, ("type %d" % t,))[0] for t in sent_types)))
    check("every requested type is in the manifest", all(t in manifests for t in sent_types))

    next_seq = {t: manifests[t].first_seq for t in sent_types}   # per type: chunks are contiguous within a type
    last_seq = begin.first_seq - 1                                 # overall: strictly increasing (global numbering)
    hashers = {t: hashlib.sha256() for t in sent_types}
    records = {t: 0 for t in sent_types}
    chunks = {t: 0 for t in sent_types}
    samples = {}
    seq_ok = crc_ok = count_ok = True
    chunks_seen = 0
    end = None
    while end is None:
        env = conn.recv()
        kind = env.WhichOneof("body")
        if kind == "snapshot_chunk":
            c = env.snapshot_chunk
            chunks_seen += 1
            seq_ok &= c.seq > last_seq and c.seq == next_seq.get(c.entity_type, c.seq)
            last_seq = c.seq
            next_seq[c.entity_type] = c.seq + 1
            crc_ok &= (zlib.crc32(c.payload) & 0xFFFFFFFF) == c.crc32
            if c.entity_type not in BATCH_BY_TYPE:
                continue  # unknown type from a newer schema: seq/CRC checked, contents skipped
            raw = decompress(c)
            name, batch_cls = BATCH_BY_TYPE[c.entity_type]
            batch = batch_cls()
            batch.ParseFromString(raw)
            count_ok &= (len(batch.items) == c.record_count) and (len(raw) == c.uncompressed_size)
            hashers.setdefault(c.entity_type, hashlib.sha256()).update(raw)
            records[c.entity_type] = records.get(c.entity_type, 0) + len(batch.items)
            chunks[c.entity_type] = chunks.get(c.entity_type, 0) + 1
            if c.entity_type not in samples and batch.items:
                samples[c.entity_type] = batch.items[0]
        elif kind == "snapshot_end":
            end = env.snapshot_end
        elif kind == "heartbeat":
            pass
        elif kind == "error":
            raise ProtocolError("stream aborted: " + error_text(env.error))
        else:
            raise ProtocolError("unexpected message while streaming: %s" % kind)
    elapsed = time.time() - started
    conn.close()

    check("chunk seq increasing and contiguous within each type from its first_seq", seq_ok)
    check("CRC-32 of every chunk payload", crc_ok)
    check("record count and uncompressed size of every chunk", count_ok)
    for t in sent_types:
        m = manifests[t]
        name = BATCH_BY_TYPE[t][0]
        check("%s: %d records as in manifest" % (name, m.record_count), records[t] == m.record_count)
        check("%s: %d chunks as in manifest" % (name, m.chunk_count), chunks[t] == m.chunk_count)
        check("%s: entity SHA-256 %s" % (name, m.entity_sha256.hex()[:16]), hashers[t].digest() == m.entity_sha256)
    whole = hashlib.sha256()
    for m in info.entities:
        whole.update(m.entity_sha256)
    check("content SHA-256 over entity hashes in manifest order", whole.digest() == info.content_sha256)
    if not wanted:
        check("SnapshotEnd content hash equals the snapshot's (unfiltered load)", end.content_sha256 == info.content_sha256)
    check("SnapshotEnd chunks_sent == %d received" % chunks_seen, end.chunks_sent == chunks_seen)
    check("SnapshotEnd records_sent == %d received" % sum(records.values()), end.records_sent == sum(records.values()))

    print()
    print("%-24s %10s %7s" % ("entity", "records", "chunks"))
    for t in sent_types:
        print("%-24s %10d %7d" % (BATCH_BY_TYPE[t][0], records[t], chunks[t]))
    print("%-24s %10d %7d" % ("total", sum(records.values()), chunks_seen))
    print("snapshot id %s, data version %d, %d bytes received in %.3f s (%.1f MB/s)" % (
        info.snapshot_id.hex(), info.data_version, conn.bytes_in, elapsed, conn.bytes_in / 1e6 / max(elapsed, 1e-9)))
    print()
    print("sample records:")
    for t in sent_types:
        if t in samples:
            print("  %s: %s" % (BATCH_BY_TYPE[t][0], str(samples[t]).strip().replace("\n", ", ")))
    passed = sum(1 for _, ok in checks if ok)
    print()
    print("%d/%d integrity checks passed" % (passed, len(checks)))
    return passed == len(checks)


def main():
    ap = argparse.ArgumentParser(description="Load a reference data snapshot and verify it.")
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=7400)
    ap.add_argument("--types", nargs="*", choices=sorted(TYPE_BY_NAME), help="entity types to request (default: all)")
    ap.add_argument("--timeout", type=float, default=60.0, help="socket timeout in seconds")
    args = ap.parse_args()
    try:
        ok = run(args)
    except (ProtocolError, OSError) as e:
        print("error: %s" % e, file=sys.stderr)
        sys.exit(1)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
