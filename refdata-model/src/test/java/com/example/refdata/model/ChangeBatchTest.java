package com.example.refdata.model;
import org.junit.Test;
import java.util.List;
import static org.junit.Assert.*;
import static com.example.refdata.model.ChangeBatch.Operation.*;
public class ChangeBatchTest {
    @Test public void crudPreservesOldView() {
        RefDataStore old = Fixtures.validSet().build().store();
        Country country = new Country("GB", "GBR", 826, "Updated name", "Europe");
        var updated = new ChangeBatch(7,8,List.of(new ChangeBatch.Change(EntityKind.COUNTRY,UPDATE,"GB",country))).apply(old);
        assertEquals("United Kingdom",old.country("GB").name());
        assertEquals("Updated name",updated.country("GB").name());
        var removed = new ChangeBatch(8,9,List.of(new ChangeBatch.Change(EntityKind.COUNTRY,DELETE,"DE",null))).apply(updated);
        assertNull(removed.country("DE"));
        var restored = new ChangeBatch(9,10,List.of(new ChangeBatch.Change(EntityKind.COUNTRY,INSERT,"DE",Fixtures.de()))).apply(removed);
        assertEquals(Fixtures.de(),restored.country("DE"));
    }
    @Test public void invalidBatchCannotChangeOriginal() {
        RefDataStore old = Fixtures.validSet().build().store();
        assertThrows(IllegalArgumentException.class, () -> new ChangeBatch(6,7,List.of(new ChangeBatch.Change(EntityKind.COUNTRY,DELETE,"DE",null))).apply(old));
        assertThrows(IllegalArgumentException.class, () -> new ChangeBatch(7,8,List.of(new ChangeBatch.Change(EntityKind.COUNTRY,INSERT,"DE",Fixtures.de()))).apply(old));
        assertThrows(com.example.refdata.model.validation.ValidationException.class, () -> new ChangeBatch(7,8,List.of(new ChangeBatch.Change(EntityKind.COUNTRY,DELETE,"GB",null))).apply(old));
        assertNotNull(old.country("GB")); assertEquals(7,old.dataVersion());
    }
}
