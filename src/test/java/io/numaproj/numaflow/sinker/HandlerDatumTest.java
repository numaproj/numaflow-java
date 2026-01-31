package io.numaproj.numaflow.sinker;

import io.numaproj.numaflow.shared.SystemMetadata;
import io.numaproj.numaflow.shared.UserMetadata;
import org.junit.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class HandlerDatumTest {

    @Test
    public void testGetKeys() {
        String[] keys = {"key1", "key2"};
        HandlerDatum datum = new HandlerDatum(keys, null, null, null, null, null, null, null);
        assertArrayEquals(keys, datum.getKeys());
    }

    @Test
    public void testGetValue() {
        byte[] value = {1, 2, 3};
        HandlerDatum datum = new HandlerDatum(null, value, null, null, null, null, null, null);
        assertArrayEquals(value, datum.getValue());
    }

    @Test
    public void testGetWatermark() {
        Instant watermark = Instant.now();
        HandlerDatum datum = new HandlerDatum(null, null, watermark, null, null, null, null, null);
        assertEquals(watermark, datum.getWatermark());
    }

    @Test
    public void testGetEventTime() {
        Instant eventTime = Instant.now();
        HandlerDatum datum = new HandlerDatum(null, null, null, eventTime, null, null, null, null);
        assertEquals(eventTime, datum.getEventTime());
    }

    @Test
    public void testGetId() {
        String id = "test-id";
        HandlerDatum datum = new HandlerDatum(null, null, null, null, id, null, null, null);
        assertEquals(id, datum.getId());
    }

    @Test
    public void testGetHeaders() {
        Map<String, String> headers = new HashMap<>();
        headers.put("header1", "value1");
        HandlerDatum datum = new HandlerDatum(null, null, null, null, null, headers, null, null);
        assertEquals(headers, datum.getHeaders());
    }

    @Test
    public void testGetUserMetadata() {
        UserMetadata userMetadata = new UserMetadata();
        HandlerDatum datum = new HandlerDatum(null, null, null, null, null, null, userMetadata, null);
        assertEquals(userMetadata, datum.getUserMetadata());
    }

    @Test
    public void testGetSystemMetadata() {
        SystemMetadata systemMetadata = new SystemMetadata();
        HandlerDatum datum = new HandlerDatum(null, null, null, null, null, null, null, systemMetadata);
        assertEquals(systemMetadata, datum.getSystemMetadata());
    }
}
