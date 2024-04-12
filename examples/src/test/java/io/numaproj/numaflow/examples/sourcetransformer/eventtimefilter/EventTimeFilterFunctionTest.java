package io.numaproj.numaflow.examples.sourcetransformer.eventtimefilter;

import io.numaproj.numaflow.sourcetransformer.Message;
import io.numaproj.numaflow.sourcetransformer.MessageList;
import io.numaproj.numaflow.sourcetransformer.SourceTransformerTestKit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

public class EventTimeFilterFunctionTest {

    @Test
    public void testServerInvocation() {
        SourceTransformerTestKit sourceTransformerTestKit = new SourceTransformerTestKit(new EventTimeFilterFunction());
        try {
            sourceTransformerTestKit.startServer();
        } catch (Exception e) {
            Assertions.fail("Failed to start server");
        }

        // Create a client which can send requests to the server
        SourceTransformerTestKit.Client client = new SourceTransformerTestKit.Client();

        SourceTransformerTestKit.TestDatum datum = SourceTransformerTestKit.TestDatum.builder()
                .eventTime(Instant.ofEpochMilli(1640995200000L))
                .value("test".getBytes())
                .build();
        MessageList result = client.sendRequest(new String[]{}, datum);

        List<Message> messages = result.getMessages();
        Assertions.assertEquals(1, messages.size());

        Assertions.assertEquals("test", new String(messages.get(0).getValue()));
        Assertions.assertEquals("within_year_2022", messages.get(0).getTags()[0]);

        try {
            client.close();
            sourceTransformerTestKit.stopServer();
        } catch (Exception e) {
            Assertions.fail("Failed to stop server");
        }
    }

    @Test
    public void testBefore2022() {
        SourceTransformerTestKit.TestDatum datum = SourceTransformerTestKit.TestDatum
                .builder()
                .eventTime(Instant.ofEpochMilli(1640995199999L))
                .build();

        EventTimeFilterFunction eventTimeFilterFunction = new EventTimeFilterFunction();
        MessageList result = eventTimeFilterFunction.processMessage(new String[]{}, datum);

        List<Message> messages = result.getMessages();
        Assertions.assertEquals(1, messages.size());

        Assertions.assertEquals(
                Message.toDrop(datum.getEventTime()).getEventTime(),
                messages.get(0).getEventTime());
        Assertions.assertEquals(0, messages.get(0).getValue().length);
        Assertions.assertEquals(
                Message.toDrop(datum.getEventTime()).getTags()[0],
                messages.get(0).getTags()[0]);
    }

    @Test
    public void testWithin2022() {
        SourceTransformerTestKit.TestDatum datum = SourceTransformerTestKit.TestDatum.builder()
                .eventTime(Instant.ofEpochMilli(1640995200000L))
                .value("test".getBytes())
                .build();

        EventTimeFilterFunction eventTimeFilterFunction = new EventTimeFilterFunction();
        MessageList result = eventTimeFilterFunction.processMessage(new String[]{}, datum);

        List<Message> messages = result.getMessages();
        Assertions.assertEquals(1, messages.size());

        Assertions.assertEquals("test", new String(messages.get(0).getValue()));
        Assertions.assertEquals("within_year_2022", messages.get(0).getTags()[0]);
    }

    @Test
    public void testAfter2022() {
        SourceTransformerTestKit.TestDatum datum = SourceTransformerTestKit.TestDatum.builder()
                .eventTime(Instant.ofEpochMilli(1672531200000L))
                .value("test".getBytes())
                .build();

        EventTimeFilterFunction eventTimeFilterFunction = new EventTimeFilterFunction();
        MessageList result = eventTimeFilterFunction.processMessage(new String[]{}, datum);

        List<Message> messages = result.getMessages();
        Assertions.assertEquals(1, messages.size());

        Assertions.assertEquals("test", new String(messages.get(0).getValue()));
        Assertions.assertEquals("after_year_2022", messages.get(0).getTags()[0]);
    }
}

