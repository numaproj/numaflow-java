package io.numaproj.numaflow.examples.source.simple;

import io.numaproj.numaflow.sourcer.Message;
import io.numaproj.numaflow.sourcer.Offset;
import io.numaproj.numaflow.sourcer.SourcerTestKit;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class SimpleSourceTest {

    @Test
    public void testServerInvocation() {
        SimpleSource simpleSource = new SimpleSource();

        SourcerTestKit sourcerTestKit = new SourcerTestKit(simpleSource);
        try {
            sourcerTestKit.startServer();
        } catch (Exception e) {
            fail("Failed to start server");
        }

        // create a client to send requests to the server
        SourcerTestKit.SourcerClient sourcerClient = new SourcerTestKit.SourcerClient();
        // create a test observer to receive messages from the server
        SourcerTestKit.TestListBasedObserver testObserver = new SourcerTestKit.TestListBasedObserver();
        // create a read request with count 10 and timeout 1 second
        SourcerTestKit.TestReadRequest testReadRequest = SourcerTestKit.TestReadRequest.builder()
                .count(10).timeout(Duration.ofSeconds(1)).build();

        try {
            sourcerClient.sendReadRequest(testReadRequest, testObserver);
            assertEquals(10, testObserver.getMessages().size());
        } catch (Exception e) {
            fail();
        }

        try {
            sourcerClient.close();
            sourcerTestKit.stopServer();
        } catch (InterruptedException e) {
            fail("Failed to stop server");
        }
    }

    @Test
    public void test_ReadAndAck() {
        SimpleSource simpleSource = new SimpleSource();
        SourcerTestKit.TestListBasedObserver testObserver = new SourcerTestKit.TestListBasedObserver();

        // Read 2 messages
        SourcerTestKit.TestReadRequest readRequest = SourcerTestKit.TestReadRequest.builder()
                .count(2).timeout(Duration.ofMillis(1000)).build();
        simpleSource.read(readRequest, testObserver);
        assertEquals(2, testObserver.getMessages().size());

        // Try reading 4 more messages
        // Since the previous batch didn't get acked, the data source shouldn't allow us to read more messages
        // We should get 0 messages, meaning the observer only holds the previous 2 messages
        SourcerTestKit.TestReadRequest readRequest2 = SourcerTestKit.TestReadRequest.builder()
                .count(2).timeout(Duration.ofMillis(1000)).build();
        simpleSource.read(readRequest2, testObserver);
        assertEquals(2, testObserver.getMessages().size());

        // Ack the first batch

        ArrayList<Offset> offsets = new ArrayList<>();
        // iterate over the testObserver messages and get the offset
        for (Message message : testObserver.getMessages()) {
            offsets.add(message.getOffset());
        }
        SourcerTestKit.TestAckRequest ackRequest = SourcerTestKit.TestAckRequest.builder()
                .offsets(offsets).build();
        simpleSource.ack(ackRequest);

        // Try reading 6 more messages
        // Since the previous batch got acked, the data source should allow us to read more messages
        // We should get 6 more messages - total of 2+6=8
        SourcerTestKit.TestReadRequest readRequest3 = SourcerTestKit.TestReadRequest.builder()
                .count(6).timeout(Duration.ofMillis(1000)).build();
        simpleSource.read(readRequest3, testObserver);
        assertEquals(8, testObserver.getMessages().size());
    }

    @Test
    public void testPending() {
        SimpleSource simpleSource = new SimpleSource();
        // simple source getPending always returns 0.
        assertEquals(0, simpleSource.getPending());
    }

}

