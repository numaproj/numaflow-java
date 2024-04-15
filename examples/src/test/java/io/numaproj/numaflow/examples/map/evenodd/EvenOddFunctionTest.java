package io.numaproj.numaflow.examples.map.evenodd;

import io.numaproj.numaflow.mapper.MapperTestKit;
import io.numaproj.numaflow.mapper.Message;
import io.numaproj.numaflow.mapper.MessageList;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;


@Slf4j
public class EvenOddFunctionTest {

    @Test
    public void testEvenNumber() {
        MapperTestKit.TestDatum datum = MapperTestKit.TestDatum
                .builder()
                .value("2".getBytes())
                .build();

        EvenOddFunction evenOddFunction = new EvenOddFunction();
        MessageList result = evenOddFunction.processMessage(new String[]{}, datum);

        List<Message> messages = result.getMessages();
        Assertions.assertEquals(1, messages.size());

        // The message should have the key "even" and tag "even-tag"
        Assertions.assertEquals("even", messages.get(0).getKeys()[0]);
        Assertions.assertEquals("even-tag", messages.get(0).getTags()[0]);
    }

    @Test
    public void testOddNumber() {
        MapperTestKit.TestDatum datum = MapperTestKit.TestDatum
                .builder()
                .value("3".getBytes())
                .build();

        EvenOddFunction evenOddFunction = new EvenOddFunction();
        MessageList result = evenOddFunction.processMessage(new String[]{}, datum);

        List<Message> messages = result.getMessages();
        Assertions.assertEquals(1, messages.size());

        // The message should have the key "odd" and tag "odd-tag"
        Assertions.assertEquals("odd", messages.get(0).getKeys()[0]);
        Assertions.assertEquals("odd-tag", messages.get(0).getTags()[0]);
    }

    @Test
    public void testNonNumeric() {
        MapperTestKit.TestDatum datum = MapperTestKit.TestDatum
                .builder()
                .value("abc".getBytes())
                .build();

        EvenOddFunction evenOddFunction = new EvenOddFunction();
        MessageList result = evenOddFunction.processMessage(new String[]{}, datum);

        List<Message> messages = result.getMessages();
        Assertions.assertEquals(1, messages.size());

        // The message should be dropped
        Assertions.assertEquals(Message.toDrop().getTags()[0], messages.get(0).getTags()[0]);
    }
}
