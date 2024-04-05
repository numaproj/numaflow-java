package io.numaproj.numaflow.examples.map.flatmap;

import io.numaproj.numaflow.examples.utils.TestDatum;
import io.numaproj.numaflow.mapper.Message;
import io.numaproj.numaflow.mapper.MessageList;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
public class FlatMapFunctionTest {

    @Test
    public void testCommaSeparatedString() {
        TestDatum datum = TestDatum.builder().value("apple,banana,carrot".getBytes()).build();

        FlatMapFunction flatMapFunction = new FlatMapFunction();
        MessageList result = flatMapFunction.processMessage(new String[]{}, datum);

        List<Message> messages = result.getMessages();
        assertEquals(3, messages.size());

        assertEquals("apple", new String(messages.get(0).getValue()));
        assertEquals("banana", new String(messages.get(1).getValue()));
        assertEquals("carrot", new String(messages.get(2).getValue()));
    }

    @Test
    public void testSingleString() {
        TestDatum datum = TestDatum.builder().value("apple".getBytes()).build();

        FlatMapFunction flatMapFunction = new FlatMapFunction();
        MessageList result = flatMapFunction.processMessage(new String[]{}, datum);

        List<Message> messages = result.getMessages();
        assertEquals(1, messages.size());

        assertEquals("apple", new String(messages.get(0).getValue()));
    }

    @Test
    public void testEmptyString() {
        TestDatum datum = TestDatum.builder().value("".getBytes()).build();

        FlatMapFunction flatMapFunction = new FlatMapFunction();
        MessageList result = flatMapFunction.processMessage(new String[]{}, datum);

        List<Message> messages = result.getMessages();
        assertEquals(1, messages.size());

        assertEquals("", new String(messages.get(0).getValue()));
    }
}

