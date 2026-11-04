package com.example.refdata.contract;

import com.example.refdata.model.Address;
import com.example.refdata.model.AddressType;
import com.example.refdata.model.ChangeBatch;
import com.example.refdata.model.ChangeBatch.Change;
import com.example.refdata.model.ChangeBatch.Operation;
import com.example.refdata.model.Country;
import com.example.refdata.model.EntityKind;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class ChangeCodecTest {
    private static final Country GB = new Country("GB", "GBR", 826, "United Kingdom", "Europe");
    private static final Address ADDRESS = new Address(7, 3, AddressType.MAILING, "L1", null, null, "London", null, "EC1", "GB", true, 18000, Address.OPEN_ENDED, 1, 1_700_000_000_000L);

    private static ChangeBatch sample() {
        return new ChangeBatch(41, 42, List.of(
                new Change(EntityKind.COUNTRY, Operation.UPDATE, "GB", GB),
                new Change(EntityKind.ADDRESS, Operation.INSERT, "7", ADDRESS),
                new Change(EntityKind.CLIENT, Operation.DELETE, "9", null)));
    }

    @Test public void roundTripKeepsEveryChange() throws IOException {
        ChangeBatch batch = sample();
        assertEquals(batch, ChangeCodec.decode(ChangeCodec.encode(batch)));
    }

    @Test public void wireLayoutIsStable() throws IOException {
        ByteBuffer b = ByteBuffer.wrap(ChangeCodec.encode(sample()));
        assertEquals(41, b.getLong());
        assertEquals(42, b.getLong());
        assertEquals(3, b.getInt());
        assertEquals(EntityKind.COUNTRY.wireCode(), b.getInt());
        assertEquals(1, b.getInt());                         // UPDATE
    }

    @Test public void malformedInputIsAnIOException() throws IOException {
        byte[] good = ChangeCodec.encode(sample());
        assertThrows(IOException.class, () -> ChangeCodec.decode(Arrays.copyOf(good, good.length - 1)));
        byte[] trailing = Arrays.copyOf(good, good.length + 1);
        assertThrows(IOException.class, () -> ChangeCodec.decode(trailing));
        byte[] badOp = good.clone();
        ByteBuffer.wrap(badOp).putInt(8 + 8 + 4 + 4, 7);    // operation of the first change
        assertThrows(IOException.class, () -> ChangeCodec.decode(badOp));
        byte[] badVersion = good.clone();
        ByteBuffer.wrap(badVersion).putLong(8, 50);          // version must be base + 1
        assertThrows(IOException.class, () -> ChangeCodec.decode(badVersion));
    }
}
