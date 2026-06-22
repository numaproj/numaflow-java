package io.numaproj.numaflow.shared;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class NackOptionsTest {

    @Test
    public void toProto_allFields() {
        NackOptions n = NackOptions.newBuilder().delay(500L).maxDeliveries(3).reason("retry").build();
        common.NackOptionsOuterClass.NackOptions p = n.toProto();
        assertTrue(p.hasDelay());
        assertEquals(500L, p.getDelay());
        assertTrue(p.hasMaxDeliveries());
        assertEquals(3, p.getMaxDeliveries());
        assertTrue(p.hasReason());
        assertEquals("retry", p.getReason());
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
        common.NackOptionsOuterClass.NackOptions p = common.NackOptionsOuterClass.NackOptions.newBuilder()
                .setDelay(500L).setMaxDeliveries(3).setReason("retry").build();
        NackOptions n = NackOptions.fromProto(p);
        assertEquals(Long.valueOf(500L), n.getDelay());
        assertEquals(Integer.valueOf(3), n.getMaxDeliveries());
        assertEquals("retry", n.getReason());
    }

    @Test
    public void fromProto_null() {
        assertNull(NackOptions.fromProto(null));
    }
}
