package io.numaproj.numaflow.examples.reduce.sum;

import io.numaproj.numaflow.reducer.Datum;
import io.numaproj.numaflow.reducer.MessageList;
import io.numaproj.numaflow.reducer.ReducerTestKit;
import org.junit.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ReduceSumTest {

    @Test
    public void testServerInvocation() {
        SumFactory sumFactory = new SumFactory();

        ReducerTestKit reducerTestKit = new ReducerTestKit(sumFactory);

        // Start the server
        try {
            reducerTestKit.startServer();
        } catch (Exception e) {
            fail("Failed to start server");
        }

        // List of datum to be sent to the server
        // create 10 datum with values 1 to 10
        List<Datum> datumList = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            datumList.add(ReducerTestKit.TestDatum
                    .builder()
                    .value(Integer.toString(i).getBytes())
                    .build());
        }

        // create a client and send requests to the server
        ReducerTestKit.ReducerClient reducerClient = new ReducerTestKit.ReducerClient();

        ReducerTestKit.TestReduceRequest testReduceRequest = ReducerTestKit.TestReduceRequest
                .builder()
                .datumList(datumList)
                .keys(new String[]{"test-key"})
                .startTime(Instant.ofEpochSecond(60000))
                .endTime(Instant.ofEpochSecond(60010))
                .build();

        try {
            MessageList messageList = reducerClient.sendReduceRequest(testReduceRequest);
            // check if the response is correct
            if (messageList.getMessages().size() != 1) {
                fail("Expected 1 message in the response");
            }
            assertEquals("55", new String(messageList.getMessages().get(0).getValue()));

        } catch (Exception e) {
            fail("Failed to send request to server");
        }
    }
}
