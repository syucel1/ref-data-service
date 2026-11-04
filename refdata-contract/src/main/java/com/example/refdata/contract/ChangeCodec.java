package com.example.refdata.contract;

import com.example.refdata.model.ChangeBatch;
import com.example.refdata.model.EntityKind;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Binary encoding of a {@link ChangeBatch}: the body of {@code Envelope.update_batch} on the TCP stream, of the admin
 * {@code POST /changes} request and of the database outbox row. Big-endian, all lengths checked on decode:
 * <pre>
 *   int64 base_version
 *   int64 version
 *   int32 change_count                       1..{@link ChangeBatch#MAX_CHANGES}
 *   change_count x {
 *     int32 entity_type                      refdata.schema.v1.EntityType number
 *     int32 operation                        0 INSERT, 1 UPDATE, 2 DELETE
 *     utf   key                              Java modified UTF-8, 2-byte length prefix
 *     int32 record_length                    0 for DELETE
 *     bytes record                           the after-image as its refdata.schema.v1 message
 *   }
 * </pre>
 * The whole encoding is at most {@link #MAX_BATCH_BYTES}.
 */
public final class ChangeCodec {
    /** Largest encoded batch; well inside the 1 MiB frame limit. */
    public static final int MAX_BATCH_BYTES = 512 * 1024;

    private static final int OP_INSERT = 0, OP_UPDATE = 1, OP_DELETE = 2;

    private ChangeCodec() {}

    /** @throws IOException when the encoding would exceed {@link #MAX_BATCH_BYTES} */
    public static byte[] encode(ChangeBatch batch) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        out.writeLong(batch.baseVersion());
        out.writeLong(batch.version());
        out.writeInt(batch.changes().size());
        for (ChangeBatch.Change change : batch.changes()) {
            out.writeInt(change.kind().wireCode());
            out.writeInt(opCode(change.operation()));
            out.writeUTF(change.key());
            byte[] record = change.record() == null ? new byte[0] : EntityCodec.toBytes(change.kind(), change.record());
            out.writeInt(record.length);
            out.write(record);
            if (bytes.size() > MAX_BATCH_BYTES) throw new IOException("batch exceeds " + MAX_BATCH_BYTES + " bytes");
        }
        return bytes.toByteArray();
    }

    /** @throws IOException when the bytes are not a well-formed, valid batch */
    public static ChangeBatch decode(byte[] bytes) throws IOException {
        if (bytes.length > MAX_BATCH_BYTES) throw new IOException("batch exceeds " + MAX_BATCH_BYTES + " bytes");
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
        long baseVersion = in.readLong();
        long version = in.readLong();
        int count = in.readInt();
        if (count < 1 || count > ChangeBatch.MAX_CHANGES) throw new IOException("invalid change count " + count);
        List<ChangeBatch.Change> changes = new ArrayList<>(count);
        try {
            for (int i = 0; i < count; i++) {
                EntityKind kind = EntityKind.fromWire(in.readInt());
                if (kind == null) throw new IOException("unknown entity type in change " + i);
                ChangeBatch.Operation operation = operation(in.readInt());
                String key = in.readUTF();
                int length = in.readInt();
                if (length < 0 || length > in.available()) throw new IOException("invalid record length " + length + " in change " + i);
                Object record = length == 0 ? null : EntityCodec.fromBytes(kind, in.readNBytes(length));
                changes.add(new ChangeBatch.Change(kind, operation, key, record));
            }
            if (in.available() != 0) throw new IOException("trailing bytes after " + count + " changes");
            return new ChangeBatch(baseVersion, version, changes);
        } catch (IllegalArgumentException e) {
            throw new IOException("invalid batch: " + e.getMessage(), e);
        }
    }

    private static int opCode(ChangeBatch.Operation operation) {
        return switch (operation) {
            case INSERT -> OP_INSERT;
            case UPDATE -> OP_UPDATE;
            case DELETE -> OP_DELETE;
        };
    }

    private static ChangeBatch.Operation operation(int code) throws IOException {
        return switch (code) {
            case OP_INSERT -> ChangeBatch.Operation.INSERT;
            case OP_UPDATE -> ChangeBatch.Operation.UPDATE;
            case OP_DELETE -> ChangeBatch.Operation.DELETE;
            default -> throw new IOException("unknown operation " + code);
        };
    }
}
