package io.numaproj.numaflow.examples.sourcetransformer.eventtimefilter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Iterator;

import io.numaproj.numaflow.examples.utils.TestDatum;
import io.numaproj.numaflow.sourcetransformer.Message;
import io.numaproj.numaflow.sourcetransformer.MessageList;
import org.junit.jupiter.api.Test;

public class EventTimeFilterFunctionTest {

    @Test
    public void testBefore2022() {
        TestDatum datum = TestDatum.builder().eventTime(Instant.ofEpochMilli(1640995199999L)).build();

        EventTimeFilterFunction eventTimeFilterFunction = new EventTimeFilterFunction();
        MessageList result = eventTimeFilterFunction.processMessage(new String[]{}, datum);

        Iterator<Message> iterator = result.getMessages().iterator();
        assertTrue(iterator.hasNext());
        Message message = iterator.next();
        assertEquals(Message.toDrop(datum.getEventTime()).getEventTime(), message.getEventTime());
        assertEquals(0, message.getValue().length);
        assertEquals(Message.toDrop(datum.getEventTime()).getTags()[0], message.getTags()[0]);
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testWithin2022() {
        TestDatum datum = TestDatum.builder()
                .eventTime(Instant.ofEpochMilli(1640995200000L))
                .value("test".getBytes())
                .build();

        EventTimeFilterFunction eventTimeFilterFunction = new EventTimeFilterFunction();
        MessageList result = eventTimeFilterFunction.processMessage(new String[]{}, datum);

        Iterator<Message> iterator = result.getMessages().iterator();
        assertTrue(iterator.hasNext());
        Message message = iterator.next();
        assertEquals("test", new String(message.getValue()));
        assertEquals("within_year_2022", message.getTags()[0]);
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testAfter2022() {
        TestDatum datum = TestDatum.builder()
                .eventTime(Instant.ofEpochMilli(1672531200000L))
                .value("test".getBytes())
                .build();

        EventTimeFilterFunction eventTimeFilterFunction = new EventTimeFilterFunction();
        MessageList result = eventTimeFilterFunction.processMessage(new String[]{}, datum);

        Iterator<Message> iterator = result.getMessages().iterator();
        assertTrue(iterator.hasNext());
        Message message = iterator.next();
        assertEquals("test", new String(message.getValue()));
        assertEquals("after_year_2022", message.getTags()[0]);
        assertFalse(iterator.hasNext());
    }
}

