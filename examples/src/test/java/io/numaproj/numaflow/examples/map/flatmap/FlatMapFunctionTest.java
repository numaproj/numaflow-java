package io.numaproj.numaflow.examples.map.flatmap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.Iterator;

import io.numaproj.numaflow.examples.utils.TestDatum;
import io.numaproj.numaflow.mapper.Message;
import io.numaproj.numaflow.mapper.MessageList;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class FlatMapFunctionTest {

    @Test
    public void testCommaSeparatedString() {
        TestDatum datum = TestDatum.builder().value("apple,banana,carrot".getBytes()).build();

        FlatMapFunction flatMapFunction = new FlatMapFunction();
        MessageList result = flatMapFunction.processMessage(new String[]{}, datum);

        Iterator<Message> iterator = result.getMessages().iterator();

        Message message = iterator.next();
        assertEquals("apple", new String(message.getValue()));

        message = iterator.next();
        assertEquals("banana", new String(message.getValue()));

        message = iterator.next();
        assertEquals("carrot", new String(message.getValue()));

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testSingleString() {
        TestDatum datum = TestDatum.builder().value("apple".getBytes()).build();

        FlatMapFunction flatMapFunction = new FlatMapFunction();
        MessageList result = flatMapFunction.processMessage(new String[]{}, datum);

        Iterator<Message> iterator = result.getMessages().iterator();
        assertTrue(iterator.hasNext());
        Message message = iterator.next();
        assertEquals("apple", new String(message.getValue()));
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testEmptyString() {
        TestDatum datum = TestDatum.builder().value("".getBytes()).build();

        FlatMapFunction flatMapFunction = new FlatMapFunction();
        MessageList result = flatMapFunction.processMessage(new String[]{}, datum);

        Iterator<Message> iterator = result.getMessages().iterator();
        assertTrue(iterator.hasNext());
        Message message = iterator.next();
        assertEquals("", new String(message.getValue()));
        assertFalse(iterator.hasNext());
    }
}

