package io.numaproj.numaflow.examples.server;

import io.numaproj.numaflow.examples.map.evenodd.EvenOddFunction;
import io.numaproj.numaflow.examples.map.flatmap.FlatMapFunction;
import io.numaproj.numaflow.examples.reduce.sum.SumFactory;
import io.numaproj.numaflow.examples.sink.simple.SimpleSink;
import io.numaproj.numaflow.examples.sourcetransformer.eventtimefilter.EventTimeFilterFunction;
import io.numaproj.numaflow.mapper.MapperTestKit;
import io.numaproj.numaflow.mapper.Message;
import io.numaproj.numaflow.mapper.MessageList;
import io.numaproj.numaflow.reducer.Datum;
import io.numaproj.numaflow.reducer.ReducerTestKit;
import io.numaproj.numaflow.sinker.Response;
import io.numaproj.numaflow.sinker.ResponseList;
import io.numaproj.numaflow.sinker.SinkerTestKit;
import io.numaproj.numaflow.sourcetransformer.SourceTransformerTestKit;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Slf4j
public class ServerTest {

  @Test
  @Order(1)
  public void testMapServerInvocation() {
    MapperTestKit mapperTestKit = new MapperTestKit(new EvenOddFunction());
    try {
      mapperTestKit.startServer();
    } catch (Exception e) {
      log.error("Failed to start server", e);
      Assertions.fail("Failed to start server");
    }

    // Create a client which can send requests to the server
    MapperTestKit.Client client = new MapperTestKit.Client();
    MapperTestKit.TestDatum datum = MapperTestKit.TestDatum.builder().value("2".getBytes()).build();
    MessageList result = client.sendRequest(new String[] {}, datum);

    List<Message> messages = result.getMessages();
    Assertions.assertEquals(1, messages.size());
    Assertions.assertEquals("even", messages.get(0).getKeys()[0]);

    try {
      client.close();
      mapperTestKit.stopServer();
    } catch (Exception e) {
      log.error("Failed to stop server", e);
      Assertions.fail("Failed to stop server");
    }
  }

  @Test
  @Order(2)
  public void testFlatMapServerInvocation() {
    MapperTestKit mapperTestKit = new MapperTestKit(new FlatMapFunction());
    try {
      mapperTestKit.startServer();
    } catch (Exception e) {
      log.error("Failed to start server", e);
    }

    MapperTestKit.Client client = new MapperTestKit.Client();
    MapperTestKit.TestDatum datum =
        MapperTestKit.TestDatum.builder().value("apple,banana,carrot".getBytes()).build();

    MessageList result = client.sendRequest(new String[] {}, datum);

    List<Message> messages = result.getMessages();
    Assertions.assertEquals(3, messages.size());

    Assertions.assertEquals("apple", new String(messages.get(0).getValue()));
    Assertions.assertEquals("banana", new String(messages.get(1).getValue()));
    Assertions.assertEquals("carrot", new String(messages.get(2).getValue()));

    try {
      client.close();
      mapperTestKit.stopServer();
    } catch (Exception e) {
      log.error("Failed to stop server", e);
    }
  }

  @Test
  @Order(3)
  public void testReduceServerInvocation() {
    SumFactory sumFactory = new SumFactory();

    ReducerTestKit reducerTestKit = new ReducerTestKit(sumFactory);

    // Start the server
    try {
      reducerTestKit.startServer();
    } catch (Exception e) {
      Assertions.fail("Failed to start server");
    }

    // List of datum to be sent to the server
    // create 10 datum with values 1 to 10
    List<Datum> datumList = new ArrayList<>();
    for (int i = 1; i <= 10; i++) {
      datumList.add(
          ReducerTestKit.TestDatum.builder().value(Integer.toString(i).getBytes()).build());
    }

    // create a client and send requests to the server
    ReducerTestKit.Client client = new ReducerTestKit.Client();

    ReducerTestKit.TestReduceRequest testReduceRequest =
        ReducerTestKit.TestReduceRequest.builder()
            .datumList(datumList)
            .keys(new String[] {"test-key"})
            .startTime(Instant.ofEpochSecond(60000))
            .endTime(Instant.ofEpochSecond(60010))
            .build();

    try {
      io.numaproj.numaflow.reducer.MessageList messageList =
          client.sendReduceRequest(testReduceRequest);
      // check if the response is correct
      if (messageList.getMessages().size() != 1) {
        Assertions.fail("Expected 1 message in the response");
      }
      Assertions.assertEquals("55", new String(messageList.getMessages().get(0).getValue()));

    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail("Failed to send request to server - ");
    }

    // Stop the server
    try {
      client.close();
      reducerTestKit.stopServer();
    } catch (InterruptedException e) {
      Assertions.fail("Failed to stop server");
    }
  }

  @Test
  @Order(4)
  public void testSinkServerInvocation() {
    int datumCount = 10;
    SinkerTestKit sinkerTestKit = new SinkerTestKit(new SimpleSink());

    // Start the server
    try {
      sinkerTestKit.startServer();
    } catch (Exception e) {
      Assertions.fail("Failed to start server");
    }

    // Create a test datum iterator with 10 messages
    SinkerTestKit.TestListIterator testListIterator = new SinkerTestKit.TestListIterator();
    for (int i = 0; i < datumCount; i++) {
      testListIterator.addDatum(
          SinkerTestKit.TestDatum.builder()
              .id("id-" + i)
              .value(("value-" + i).getBytes())
              .headers(Map.of("test-key", "test-value"))
              .build());
    }

    SinkerTestKit.Client client = new SinkerTestKit.Client();
    try {
      ResponseList responseList = client.sendRequest(testListIterator);
      Assertions.assertEquals(datumCount, responseList.getResponses().size());
      for (Response response : responseList.getResponses()) {
        Assertions.assertEquals(true, response.getSuccess());
      }
    } catch (Exception e) {
      Assertions.fail("Failed to send requests");
    }

    // Stop the server
    try {
      client.close();
      sinkerTestKit.stopServer();
    } catch (InterruptedException e) {
      Assertions.fail("Failed to stop server");
    }

    // we can add the logic to verify if the messages were
    // successfully written to the sink(could be a file, database, etc.)
  }

  // FIXME: once tester kit changes are done for bidirectional streaming source
  //    @Ignore
  //    @Test
  //    @Order(5)
  //    public void testSourceServerInvocation() {
  //        SimpleSource simpleSource = new SimpleSource();
  //
  //        SourcerTestKit sourcerTestKit = new SourcerTestKit(simpleSource);
  //        try {
  //            sourcerTestKit.startServer();
  //        } catch (Exception e) {
  //            Assertions.fail("Failed to start server");
  //        }
  //
  //        // create a client to send requests to the server
  //        SourcerTestKit.Client sourcerClient = new SourcerTestKit.Client();
  //        // create a test observer to receive messages from the server
  //        SourcerTestKit.TestListBasedObserver testObserver = new
  // SourcerTestKit.TestListBasedObserver();
  //        // create a read request with count 10 and timeout 1 second
  //        SourcerTestKit.TestReadRequest testReadRequest =
  // SourcerTestKit.TestReadRequest.builder()
  //                .count(10).timeout(Duration.ofSeconds(1)).build();
  //
  //        try {
  //            sourcerClient.sendReadRequest(testReadRequest, testObserver);
  //            Assertions.assertEquals(10, testObserver.getMessages().size());
  //        } catch (Exception e) {
  //            Assertions.fail("Failed to send request to server");
  //        }
  //
  //        try {
  //            sourcerClient.close();
  //            sourcerTestKit.stopServer();
  //        } catch (InterruptedException e) {
  //            Assertions.fail("Failed to stop server");
  //        }
  //    }

  @Test
  @Order(6)
  public void testSourceTransformerServerInvocation() {
    SourceTransformerTestKit sourceTransformerTestKit =
        new SourceTransformerTestKit(new EventTimeFilterFunction());
    try {
      sourceTransformerTestKit.startServer();
    } catch (Exception e) {
      Assertions.fail("Failed to start server");
    }

    // Create a client which can send requests to the server
    SourceTransformerTestKit.Client client = new SourceTransformerTestKit.Client();

    SourceTransformerTestKit.TestDatum datum =
        SourceTransformerTestKit.TestDatum.builder()
            .eventTime(Instant.ofEpochMilli(1640995200000L))
            .value("test".getBytes())
            .build();
    io.numaproj.numaflow.sourcetransformer.MessageList result =
        client.sendRequest(new String[] {}, datum);

    List<io.numaproj.numaflow.sourcetransformer.Message> messages = result.getMessages();
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
}
