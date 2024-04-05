package io.numaproj.numaflow.examples.sourcetransformer.eventtimefilter;

import io.numaproj.numaflow.examples.utils.TestDatum;
import io.numaproj.numaflow.sourcetransformer.Message;
import io.numaproj.numaflow.sourcetransformer.MessageList;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class EventTimeFilterFunctionTest {

    @Test
    public void testBefore2022() {
        TestDatum datum = TestDatum
                .builder()
                .eventTime(Instant.ofEpochMilli(1640995199999L))
                .build();

        EventTimeFilterFunction eventTimeFilterFunction = new EventTimeFilterFunction();
        MessageList result = eventTimeFilterFunction.processMessage(new String[]{}, datum);

        List<Message> messages = result.getMessages();
        assertEquals(1, messages.size());

        assertEquals(
                Message.toDrop(datum.getEventTime()).getEventTime(),
                messages.get(0).getEventTime());
        assertEquals(0, messages.get(0).getValue().length);
        assertEquals(
                Message.toDrop(datum.getEventTime()).getTags()[0],
                messages.get(0).getTags()[0]);
    }

    @Test
    public void testWithin2022() {
        TestDatum datum = TestDatum.builder()
                .eventTime(Instant.ofEpochMilli(1640995200000L))
                .value("test".getBytes())
                .build();

        EventTimeFilterFunction eventTimeFilterFunction = new EventTimeFilterFunction();
        MessageList result = eventTimeFilterFunction.processMessage(new String[]{}, datum);

        List<Message> messages = result.getMessages();
        assertEquals(1, messages.size());

        assertEquals("test", new String(messages.get(0).getValue()));
        assertEquals("within_year_2022", messages.get(0).getTags()[0]);
    }

    @Test
    public void testAfter2022() {
        TestDatum datum = TestDatum.builder()
                .eventTime(Instant.ofEpochMilli(1672531200000L))
                .value("test".getBytes())
                .build();

        EventTimeFilterFunction eventTimeFilterFunction = new EventTimeFilterFunction();
        MessageList result = eventTimeFilterFunction.processMessage(new String[]{}, datum);

        List<Message> messages = result.getMessages();
        assertEquals(1, messages.size());

        assertEquals("test", new String(messages.get(0).getValue()));
        assertEquals("after_year_2022", messages.get(0).getTags()[0]);
    }
}

