package io.numaproj.numaflow.batchmapper;

import org.junit.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class HandlerDatumTest {

    @Test
    public void testGetKeys() {
        String[] keys = {"key1", "key2"};
        HandlerDatum datum = new HandlerDatum(keys, null, null, null, null, null);
        assertArrayEquals(keys, datum.getKeys());
    }

    @Test
    public void testGetValue() {
        byte[] value = {1, 2, 3};
        HandlerDatum datum = new HandlerDatum(null, value, null, null, null, null);
        assertArrayEquals(value, datum.getValue());
    }

    @Test
    public void testGetWatermark() {
        Instant watermark = Instant.now();
        HandlerDatum datum = new HandlerDatum(null, null, watermark, null, null, null);
        assertEquals(watermark, datum.getWatermark());
    }

    @Test
    public void testGetEventTime() {
        Instant eventTime = Instant.now();
        HandlerDatum datum = new HandlerDatum(null, null, null, eventTime, null, null);
        assertEquals(eventTime, datum.getEventTime());
    }

    @Test
    public void testGetId() {
        String id = "test-id";
        HandlerDatum datum = new HandlerDatum(null, null, null, null, id, null);
        assertEquals(id, datum.getId());
    }

    @Test
    public void testGetHeaders() {
        Map<String, String> headers = new HashMap<>();
        headers.put("header1", "value1");
        HandlerDatum datum = new HandlerDatum(null, null, null, null, null, headers);
        assertEquals(headers, datum.getHeaders());
    }

    @Test
    public void testEOF_DATUM() {
        assertNull(HandlerDatum.EOF_DATUM.getKeys());
        assertNull(HandlerDatum.EOF_DATUM.getValue());
        assertNull(HandlerDatum.EOF_DATUM.getWatermark());
        assertNull(HandlerDatum.EOF_DATUM.getEventTime());
        assertNull(HandlerDatum.EOF_DATUM.getId());
        assertNull(HandlerDatum.EOF_DATUM.getHeaders());
    }
}
