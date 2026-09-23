package io.numaproj.numaflow.accumulator.model;

import org.junit.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class MessageTest {

    @Test
    public void testMessageFromDatum() {
        Datum datum = buildDatum();
        Message message = new Message(datum);

        assertArrayEquals("hello".getBytes(), message.getValue());
        assertArrayEquals(new String[]{"key1", "key2"}, message.getKeys());
        assertArrayEquals(null, message.getTags());
        assertEquals("test-id", message.getId());
        assertEquals(Instant.ofEpochMilli(60000), message.getEventTime());
        assertEquals(Instant.ofEpochMilli(59000), message.getWatermark());
    }

    @Test
    public void testToDrop() {
        Datum datum = buildDatum();
        Message message = Message.toDrop(datum);

        // The DROP tag must be set so the message is not forwarded downstream.
        String[] dropTags = {"U+005C__DROP__"};
        assertArrayEquals(dropTags, message.getTags());
        // No value is forwarded, but the identifying/watermark fields are carried over so the
        // accumulator can advance the watermark and release the tracked state.
        assertArrayEquals(new byte[0], message.getValue());
        assertArrayEquals(new String[]{"key1", "key2"}, message.getKeys());
        assertEquals("test-id", message.getId());
        assertEquals(Instant.ofEpochMilli(60000), message.getEventTime());
        assertEquals(Instant.ofEpochMilli(59000), message.getWatermark());
    }

    private Datum buildDatum() {
        Map<String, String> headers = new HashMap<>();
        headers.put("x", "y");
        return new TestDatum(
                new String[]{"key1", "key2"},
                "hello".getBytes(),
                Instant.ofEpochMilli(59000),
                Instant.ofEpochMilli(60000),
                headers,
                "test-id");
    }

    private static class TestDatum implements Datum {
        private final String[] keys;
        private final byte[] value;
        private final Instant watermark;
        private final Instant eventTime;
        private final Map<String, String> headers;
        private final String id;

        TestDatum(
                String[] keys,
                byte[] value,
                Instant watermark,
                Instant eventTime,
                Map<String, String> headers,
                String id) {
            this.keys = keys;
            this.value = value;
            this.watermark = watermark;
            this.eventTime = eventTime;
            this.headers = headers;
            this.id = id;
        }

        @Override
        public byte[] getValue() {
            return value;
        }

        @Override
        public String[] getKeys() {
            return keys;
        }

        @Override
        public Instant getEventTime() {
            return eventTime;
        }

        @Override
        public Instant getWatermark() {
            return watermark;
        }

        @Override
        public Map<String, String> getHeaders() {
            return headers;
        }

        @Override
        public String getID() {
            return id;
        }
    }
}
