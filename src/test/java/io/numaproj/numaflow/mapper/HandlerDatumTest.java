package io.numaproj.numaflow.mapper;

import io.numaproj.numaflow.shared.SystemMetadata;
import io.numaproj.numaflow.shared.UserMetadata;
import org.junit.Test;

import java.time.Instant;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class HandlerDatumTest {
    @Test
    public void testHandlerDatum() {
        Instant watermark = Instant.now();
        Instant eventTime = Instant.now();
        HashMap<String, String> headers = new HashMap<>();
        headers.put("header1", "value1");
        HandlerDatum datum = new HandlerDatum("asdf".getBytes(), watermark, eventTime, headers, new UserMetadata(), new SystemMetadata());
        assertEquals(watermark, datum.getWatermark());
        assertEquals(eventTime, datum.getEventTime());
        assertEquals(headers, datum.getHeaders());
    }
}
