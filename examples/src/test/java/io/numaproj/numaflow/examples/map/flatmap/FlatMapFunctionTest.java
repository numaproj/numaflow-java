package io.numaproj.numaflow.examples.map.flatmap;

import io.numaproj.numaflow.mapper.MapperTestKit;
import io.numaproj.numaflow.mapper.Message;
import io.numaproj.numaflow.mapper.MessageList;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;


@Slf4j
public class FlatMapFunctionTest {

    @Test
    public void testSingleString() {
        MapperTestKit.TestDatum datum = MapperTestKit.TestDatum
                .builder()
                .value("apple".getBytes())
                .build();

        FlatMapFunction flatMapFunction = new FlatMapFunction();
        MessageList result = flatMapFunction.processMessage(new String[]{}, datum);

        List<Message> messages = result.getMessages();
        Assertions.assertEquals(1, messages.size());

        Assertions.assertEquals("apple", new String(messages.get(0).getValue()));
    }

    @Test
    public void testEmptyString() {
        MapperTestKit.TestDatum datum = MapperTestKit.TestDatum
                .builder()
                .value("".getBytes())
                .build();

        FlatMapFunction flatMapFunction = new FlatMapFunction();
        MessageList result = flatMapFunction.processMessage(new String[]{}, datum);

        List<Message> messages = result.getMessages();
        Assertions.assertEquals(1, messages.size());

        Assertions.assertEquals("", new String(messages.get(0).getValue()));
    }
}

