package io.numaproj.numaflow.shared;

import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class NackOptionsTest {

    @Test
    public void toProto_allFields() {
        HashMap<String, String> nackMap = new HashMap<>();
        nackMap.put("key", "value");
        NackOptions n = NackOptions.newBuilder().delay(500L).maxDeliveries(3).reason("retry").nackMap(nackMap).build();
        common.NackOptionsOuterClass.NackOptions p = n.toProto();
        assertTrue(p.hasDelay());
        assertEquals(500L, p.getDelay());
        assertTrue(p.hasMaxDeliveries());
        assertEquals(3, p.getMaxDeliveries());
        assertTrue(p.hasReason());
        assertEquals("retry", p.getReason());
        assertFalse(p.getNackMapMap().isEmpty());
        assertEquals("value", p.getNackMapMap().get("key"));
    }

    @Test
    public void toProto_partialFields() {
        NackOptions n = NackOptions.newBuilder().delay(100L).build();
        common.NackOptionsOuterClass.NackOptions p = n.toProto();
        assertTrue(p.hasDelay());
        assertFalse(p.hasMaxDeliveries());
        assertFalse(p.hasReason());
    }

    @Test
    public void fromProto_roundTrip() {
        HashMap<String, String> nackMap = new HashMap<>();
        nackMap.put("key", "value");
        common.NackOptionsOuterClass.NackOptions p = common.NackOptionsOuterClass.NackOptions.newBuilder()
                .setDelay(500L).setMaxDeliveries(3).setReason("retry").putAllNackMap(nackMap).build();
        NackOptions n = NackOptions.fromProto(p);
        assertEquals(Long.valueOf(500L), n.getDelay());
        assertEquals(Integer.valueOf(3), n.getMaxDeliveries());
        assertEquals("retry", n.getReason());
        assertEquals(nackMap, n.getNackMap());
    }

    @Test
    public void fromProto_null() {
        assertNull(NackOptions.fromProto(null));
    }
}
