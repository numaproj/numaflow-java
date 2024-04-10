package io.numaproj.numaflow.examples.sink.simple;

import io.numaproj.numaflow.sinker.Response;
import io.numaproj.numaflow.sinker.ResponseList;
import io.numaproj.numaflow.sinker.SinkerTestKit;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Slf4j
public class SimpleSinkTest {

    @Test
    public void testServerInvocation() {
        int datumCount = 10;
        SinkerTestKit sinkerTestKit = new SinkerTestKit(new SimpleSink());

        // Start the server
        try {
            sinkerTestKit.startServer();
        } catch (Exception e) {
            fail("Failed to start server");
        }

        // Create a test datum iterator with 10 messages
        SinkerTestKit.TestDatumIterator testDatumIterator = new SinkerTestKit.TestDatumIterator();
        for (int i = 0; i < datumCount; i++) {
            testDatumIterator.addDatum(SinkerTestKit.TestDatum
                    .builder()
                    .id("id-" + i)
                    .value(("value-" + i).getBytes())
                    .build());
        }

        try {
            ResponseList responseList = sinkerTestKit.sendRequests(testDatumIterator);
            assertEquals(datumCount, responseList.getResponses().size());
            for (Response response: responseList.getResponses()) {
                assertEquals(true, response.getSuccess());
            }
        } catch (Exception e) {
            fail("Failed to send requests");
        }

        // Stop the server
        try {
            sinkerTestKit.stopServer();
        } catch (InterruptedException e) {
            fail("Failed to stop server");
        }

        // we can add the logic to verify if the messages were
        // successfully written to the sink(could be a file, database, etc.)
    }
}
