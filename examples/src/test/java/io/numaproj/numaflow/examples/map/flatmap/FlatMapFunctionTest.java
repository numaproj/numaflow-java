package io.numaproj.numaflow.examples.map.flatmap;

import io.numaproj.numaflow.mapper.MapperTestKit;
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
        MapperTestKit mapperTestKit = new MapperTestKit(new FlatMapFunction());
        try {
            mapperTestKit.startServer();
        } catch (Exception e) {
            log.error("Failed to start server", e);
        }

        MapperTestKit.Client client = new MapperTestKit.Client();
        MapperTestKit.TestDatum datum = MapperTestKit.TestDatum
                .builder()
                .value("apple,banana,carrot".getBytes())
                .build();

        MessageList result = client.sendRequest(new String[]{}, datum);

        List<Message> messages = result.getMessages();
        assertEquals(3, messages.size());

        assertEquals("apple", new String(messages.get(0).getValue()));
        assertEquals("banana", new String(messages.get(1).getValue()));
        assertEquals("carrot", new String(messages.get(2).getValue()));

        try {
            mapperTestKit.stopServer();
        } catch (Exception e) {
            log.error("Failed to stop server", e);
        }
    }

    @Test
    public void testSingleString() {
        MapperTestKit.TestDatum datum = MapperTestKit.TestDatum
                .builder()
                .value("apple".getBytes())
                .build();

        FlatMapFunction flatMapFunction = new FlatMapFunction();
        MessageList result = flatMapFunction.processMessage(new String[]{}, datum);

        List<Message> messages = result.getMessages();
        assertEquals(1, messages.size());

        assertEquals("apple", new String(messages.get(0).getValue()));
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
        assertEquals(1, messages.size());

        assertEquals("", new String(messages.get(0).getValue()));
    }
}

