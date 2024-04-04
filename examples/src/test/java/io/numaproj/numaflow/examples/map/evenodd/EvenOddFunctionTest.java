package io.numaproj.numaflow.examples.map.evenodd;


import io.numaproj.numaflow.mapper.Datum;
import io.numaproj.numaflow.mapper.Message;
import io.numaproj.numaflow.mapper.MessageList;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Slf4j
public class EvenOddFunctionTest {

    @Test
    public void testEvenNumber() {
        Datum datum = mock(Datum.class);
        when(datum.getValue()).thenReturn("2".getBytes());

        EvenOddFunction evenOddFunction = new EvenOddFunction();
        MessageList result = evenOddFunction.processMessage(new String[]{}, datum);

        Iterator<Message> iterator = result.getMessages().iterator();
        Message message = iterator.hasNext() ? iterator.next() : null;

        // The message should have the key "even" and tag "even-tag"
        assert message != null;
        assertEquals("even", message.getKeys()[0]);
        assertEquals("even-tag", message.getTags()[0]);
    }

    @Test
    public void testOddNumber() {
        Datum datum = mock(Datum.class);
        when(datum.getValue()).thenReturn("3".getBytes());

        EvenOddFunction evenOddFunction = new EvenOddFunction();
        MessageList result = evenOddFunction.processMessage(new String[]{}, datum);

        Iterator<Message> iterator = result.getMessages().iterator();
        Message message = iterator.hasNext() ? iterator.next() : null;

        // The message should have the key "odd" and tag "odd-tag"
        assert message != null;
        assertEquals("odd", message.getKeys()[0]);
        assertEquals("odd-tag", message.getTags()[0]);
    }

    @Test
    public void testNonNumeric() {
        Datum datum = mock(Datum.class);
        when(datum.getValue()).thenReturn("abc".getBytes());

        EvenOddFunction evenOddFunction = new EvenOddFunction();
        MessageList result = evenOddFunction.processMessage(new String[]{}, datum);

        Iterator<Message> iterator = result.getMessages().iterator();
        Message message = iterator.hasNext() ? iterator.next() : null;

        // The message should be dropped
        assert message != null;
        assertEquals(Message.toDrop().getTags()[0], message.getTags()[0]);
    }
}
