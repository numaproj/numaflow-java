package io.numaproj.numaflow.examples.sourcetransformer.eventtimefilter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Iterator;

import io.numaproj.numaflow.sourcetransformer.Datum;
import io.numaproj.numaflow.sourcetransformer.Message;
import io.numaproj.numaflow.sourcetransformer.MessageList;
import org.junit.jupiter.api.Test;

public class EventTimeFilterFunctionTest {

    @Test
    public void testBefore2022() {
        Datum datum = mock(Datum.class);
        when(datum.getEventTime()).thenReturn(Instant.ofEpochMilli(1640995199999L));

        EventTimeFilterFunction eventTimeFilterFunction = new EventTimeFilterFunction();
        MessageList result = eventTimeFilterFunction.processMessage(new String[]{}, datum);

        Iterator<Message> iterator = result.getMessages().iterator();
        Message message = iterator.hasNext() ? iterator.next() : null;
        assert message != null;
        assertEquals(Message.toDrop(datum.getEventTime()).getEventTime(), message.getEventTime());
        assertEquals(0, message.getValue().length);
        assertEquals(Message.toDrop(datum.getEventTime()).getTags()[0], message.getTags()[0]);
    }

    @Test
    public void testWithin2022() {
        Datum datum = mock(Datum.class);
        when(datum.getEventTime()).thenReturn(Instant.ofEpochMilli(1640995200000L));
        when(datum.getValue()).thenReturn("test".getBytes());

        EventTimeFilterFunction eventTimeFilterFunction = new EventTimeFilterFunction();
        MessageList result = eventTimeFilterFunction.processMessage(new String[]{}, datum);

        Iterator<Message> iterator = result.getMessages().iterator();
        Message message = iterator.hasNext() ? iterator.next() : null;
        assert message != null;
        assertEquals("test", new String(message.getValue()));
        assertEquals("within_year_2022", message.getTags()[0]);
    }

    @Test
    public void testAfter2022() {
        Datum datum = mock(Datum.class);
        when(datum.getEventTime()).thenReturn(Instant.ofEpochMilli(1672531200000L));
        when(datum.getValue()).thenReturn("test".getBytes());

        EventTimeFilterFunction eventTimeFilterFunction = new EventTimeFilterFunction();
        MessageList result = eventTimeFilterFunction.processMessage(new String[]{}, datum);

        Iterator<Message> iterator = result.getMessages().iterator();
        Message message = iterator.hasNext() ? iterator.next() : null;
        assert message != null;
        assertEquals("test", new String(message.getValue()));
        assertEquals("after_year_2022", message.getTags()[0]);
    }
}

