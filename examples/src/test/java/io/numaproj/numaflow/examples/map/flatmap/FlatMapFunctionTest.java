package io.numaproj.numaflow.examples.map.flatmap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Iterator;

import io.numaproj.numaflow.mapper.Datum;
import io.numaproj.numaflow.mapper.Message;
import io.numaproj.numaflow.mapper.MessageList;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class FlatMapFunctionTest {

    @Test
    public void testCommaSeparatedString() {
        Datum datum = mock(Datum.class);
        when(datum.getValue()).thenReturn("apple,banana,carrot".getBytes());

        FlatMapFunction flatMapFunction = new FlatMapFunction();
        MessageList result = flatMapFunction.processMessage(new String[]{}, datum);

        Iterator<Message> iterator = result.getMessages().iterator();

        Message message = iterator.hasNext() ? iterator.next() : null;
        assertEquals("apple", new String(message.getValue()));

        message = iterator.hasNext() ? iterator.next() : null;
        assertEquals("banana", new String(message.getValue()));

        message = iterator.hasNext() ? iterator.next() : null;
        assertEquals("carrot", new String(message.getValue()));
    }

    @Test
    public void testSingleString() {
        Datum datum = mock(Datum.class);
        when(datum.getValue()).thenReturn("apple".getBytes());

        FlatMapFunction flatMapFunction = new FlatMapFunction();
        MessageList result = flatMapFunction.processMessage(new String[]{}, datum);

        Iterator<Message> iterator = result.getMessages().iterator();
        Message message = iterator.hasNext() ? iterator.next() : null;
        assertEquals("apple", new String(message.getValue()));
    }

    @Test
    public void testEmptyString() {
        Datum datum = mock(Datum.class);
        when(datum.getValue()).thenReturn("".getBytes());

        FlatMapFunction flatMapFunction = new FlatMapFunction();
        MessageList result = flatMapFunction.processMessage(new String[]{}, datum);

        Iterator<Message> iterator = result.getMessages().iterator();
        Message message = iterator.hasNext() ? iterator.next() : null;
        assertEquals("", new String(message.getValue()));
    }
}

