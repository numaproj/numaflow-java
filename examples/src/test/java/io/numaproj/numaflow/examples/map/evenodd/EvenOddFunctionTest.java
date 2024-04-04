package io.numaproj.numaflow.examples.map.evenodd;


import io.numaproj.numaflow.examples.utils.TestDatum;
import io.numaproj.numaflow.mapper.Message;
import io.numaproj.numaflow.mapper.MessageList;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
public class EvenOddFunctionTest {

    @Test
    public void testEvenNumber() {
        TestDatum datum = TestDatum.builder().value("2".getBytes()).build();

        EvenOddFunction evenOddFunction = new EvenOddFunction();
        MessageList result = evenOddFunction.processMessage(new String[]{}, datum);

        Iterator<Message> iterator = result.getMessages().iterator();
        assertTrue(iterator.hasNext());
        Message message = iterator.next();
        assertNotNull(message);

        // The message should have the key "even" and tag "even-tag"
        assertEquals("even", message.getKeys()[0]);
        assertEquals("even-tag", message.getTags()[0]);

        // No more messages
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testOddNumber() {
        TestDatum datum = TestDatum.builder().value("3".getBytes()).build();

        EvenOddFunction evenOddFunction = new EvenOddFunction();
        MessageList result = evenOddFunction.processMessage(new String[]{}, datum);

        Iterator<Message> iterator = result.getMessages().iterator();
        assertTrue(iterator.hasNext());
        Message message = iterator.next();
        assertNotNull(message);

        // The message should have the key "odd" and tag "odd-tag"
        assert message != null;
        assertEquals("odd", message.getKeys()[0]);
        assertEquals("odd-tag", message.getTags()[0]);

        // No more messages
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testNonNumeric() {
        TestDatum datum = TestDatum.builder().value("abc".getBytes()).build();

        EvenOddFunction evenOddFunction = new EvenOddFunction();
        MessageList result = evenOddFunction.processMessage(new String[]{}, datum);

        Iterator<Message> iterator = result.getMessages().iterator();
        assertTrue(iterator.hasNext());
        Message message = iterator.next();
        assertNotNull(message);

        // The message should be dropped
        assert message != null;
        assertEquals(Message.toDrop().getTags()[0], message.getTags()[0]);

        // No more messages
        assertFalse(iterator.hasNext());
    }
}
